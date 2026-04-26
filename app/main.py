import os
import uuid
import json
import asyncio
import time
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from app.langfuse_client import get_langfuse_client, flush, is_enabled, trace_context
from app.monitoring import setup_logging, RateLimiter, RequestMonitor, timed_operation

# Setup logging
LOG_DIR = os.environ.get("LOG_DIR", "logs")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = setup_logging(LOG_DIR, LOG_LEVEL)

# Create request monitor
request_monitor = RequestMonitor(logger)

from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from sse_starlette import EventSourceResponse

from app.models import (
    UploadResponse,
    SchemaInfo,
    DataQualityReport,
    QueryRequest,
    QueryResponse,
    ExploreRequest,
    SignalResponse,
    HypothesisRequest,
    HypothesisGraphResponse,
    ExecuteRequest,
    ExecutionResponse,
    Phase4Request,
    Phase4Response,
    ChatRequest,
    ChatResponse,
)
from app.layers import (
    FileIntakeLayer,
    SchemaValidator,
    MissingnessProfiler,
    OutlierScanner,
    DataFrameMaterializer,
    SemanticMapper,
    DuckDBConnector,
)
from app.explore import SignalOrchestrator
from app.hypothesis import HypothesisOrchestrator
from app.execute import ExecutionEngine, ExecutionStore
from app.phase4 import Phase4Orchestrator, ChatInterface, _get_env_int, _get_env_bool
from app.chat_router import IntelligentChatRouter, detect_question_type, build_execution_plan
from app.progress import progress_store, progress_generator
from datetime import datetime

# Import new layer routes
from app.api.nlq_routes import router as nlq_router
from app.api.report_routes import router as report_router

app = FastAPI(title="Data Analyst Agent")

# Register new routes
app.include_router(nlq_router)
app.include_router(report_router)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8015", "http://127.0.0.1:8015", "http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:3002", "http://127.0.0.1:3002"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
RATE_LIMIT_RPM = int(os.environ.get("RATE_LIMIT_RPM", 60))
RATE_LIMIT_BURST = int(os.environ.get("RATE_LIMIT_BURST", 10))
app.add_middleware(RateLimiter, requests_per_minute=RATE_LIMIT_RPM, burst=RATE_LIMIT_BURST)

logger.info(f"Rate limiting enabled: {RATE_LIMIT_RPM} req/min, burst: {RATE_LIMIT_BURST}")


# Serve HTML frontend
@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse("code.html")

@app.get("/code.html", include_in_schema=False)
async def serve_code_html():
    return FileResponse("code.html")


@app.middleware("http")
async def track_requests(request, call_next):
    """Track all HTTP requests for metrics."""
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    # Track in request_monitor
    request_monitor.total_requests += 1
    request_monitor.total_latency += duration
    request_monitor.endpoint_counts[f"{request.method} {request.url.path}"] += 1
    
    if response.status_code >= 400:
        request_monitor.total_errors += 1
    
    if duration > 1.0:
        logger.warning(f"Slow request: {request.method} {request.url.path} took {duration:.2f}s")
    
    response.headers["X-Response-Time"] = f"{duration:.3f}s"
    return response


DATA_DIR = os.environ.get("DATA_DIR", "data")

file_intake = FileIntakeLayer(DATA_DIR)
schema_validator = SchemaValidator()
missingness_profiler = MissingnessProfiler()
outlier_scanner = OutlierScanner()
materializer = DataFrameMaterializer(DATA_DIR)
semantic_mapper = SemanticMapper()
duckdb_connector = DuckDBConnector(DATA_DIR)
signal_orchestrator = SignalOrchestrator(DATA_DIR)
hypothesis_orchestrator = HypothesisOrchestrator(DATA_DIR)

_session_schemas: dict = {}
_session_quality: dict = {}

execution_engine = ExecutionEngine(DATA_DIR, duckdb_connector, _session_schemas)
execution_store = ExecutionStore(DATA_DIR)
phase4_orchestrator = Phase4Orchestrator(DATA_DIR)
chat_interface = ChatInterface(DATA_DIR)
intelligent_router = IntelligentChatRouter(DATA_DIR)

_phase_executors = {}


@app.post("/upload_csv", response_model=UploadResponse)
async def upload_csv(file: UploadFile, background_tasks: BackgroundTasks):
    session_id = str(uuid.uuid4())
    content = await file.read()

    checksum, file_size = file_intake.store_csv(session_id, content)

    import pandas as pd
    df = pd.read_csv(file_intake.get_raw_path(session_id))

    normalized_columns = schema_validator.normalize_columns(list(df.columns))
    df.columns = normalized_columns

    inferred_types = {}
    for col in df.columns:
        inferred_types[col] = schema_validator.infer_type(df[col].dropna().tolist())

    _session_schemas[session_id] = {
        "columns": normalized_columns,
        "inferred_types": inferred_types,
    }

    valid_df, quarantine_df = schema_validator.enforce_types(df, inferred_types)

    missingness = missingness_profiler.compute_missingness(valid_df)
    outliers = outlier_scanner.detect_outliers(valid_df)

    valid_count, quarantine_count = materializer.materialize(
        session_id, valid_df, quarantine_df
    )

    _session_quality[session_id] = {
        "missingness": missingness,
        "outliers": outliers,
        "total_rows": len(df),
        "valid_rows": valid_count,
        "quarantined_rows": quarantine_count,
    }

    # Start pipeline in background and return immediately with processing status
    def _run_pipeline_back(sid: str):
        try:
            progress_store.set_phase(sid, "explore", "Starting signal detection (background)")
            signals = signal_orchestrator.run_exploration(sid, df)

            progress_store.set_phase(sid, "hypotheses", "Building hypothesis graph")
            schema = _session_schemas.get(sid, {})
            hypothesis_graph = hypothesis_orchestrator.build_hypotheses(sid, signals, schema)

            progress_store.set_phase(sid, "execute", "Executing hypotheses")

            # Execution with retries
            exec_attempts = 0
            execution_result = None
            while exec_attempts < 3:
                try:
                    execution_result = execution_engine.run(sid, hypothesis_graph)
                    execution_store.save_execution(sid, execution_result)
                    break
                except Exception as e:
                    exec_attempts += 1
                    progress_store.set_phase(sid, "execute", f"Execution error, retry {exec_attempts}: {str(e)}")
                    time.sleep(1 + exec_attempts)

            if execution_result is None:
                raise RuntimeError("Execution failed after retries")

            progress_store.set_phase(sid, "phase4", "Generating insights and plots")

            # Phase4 with retries
            phase4_attempts = 0
            phase4_result = None
            while phase4_attempts < 2:
                try:
                    phase4_result = phase4_orchestrator.run(sid, execution_result, hypothesis_graph, schema)
                    break
                except Exception as e:
                    phase4_attempts += 1
                    progress_store.set_phase(sid, "phase4", f"Phase4 error, retry {phase4_attempts}: {str(e)}")
                    time.sleep(1 + phase4_attempts)

            if phase4_result is None:
                raise RuntimeError("Phase4 failed after retries")

            progress_store.set_phase(sid, "completed", "Analysis complete")
        except Exception as e:
            progress_store.set_phase(sid, "error", f"Pipeline error: {str(e)}")

    background_tasks.add_task(_run_pipeline_back, session_id)

    first_screen = {"status": "processing", "progress_endpoint": f"/progress/{session_id}", "first_screen": None}

    return UploadResponse(
        session_id=session_id,
        checksum=checksum,
        file_size=file_size,
        message="CSV uploaded and processing started",
        first_screen=first_screen,
    )


@app.get("/schema/{session_id}", response_model=SchemaInfo)
def get_schema(session_id: str):
    if session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    schema = _session_schemas[session_id]
    return SchemaInfo(
        columns=schema["columns"],
        inferred_types=schema["inferred_types"],
    )


@app.get("/data_quality/{session_id}", response_model=DataQualityReport)
def get_data_quality(session_id: str):
    if session_id not in _session_quality:
        raise HTTPException(status_code=404, detail="Session not found")

    quality = _session_quality[session_id]
    return DataQualityReport(
        missingness=quality["missingness"],
        outliers=quality["outliers"],
        total_rows=quality["total_rows"],
        valid_rows=quality["valid_rows"],
        quarantined_rows=quality["quarantined_rows"],
    )


@app.post("/query", response_model=QueryResponse)
def query_data(request: QueryRequest):
    if request.session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        data = duckdb_connector.query(request.session_id, request.sql)
        columns = duckdb_connector.get_columns(request.session_id)

        return QueryResponse(
            data=data,
            columns=columns,
            row_count=len(data),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "version": os.environ.get("APP_VERSION", "1.0.0")
    }


@app.get("/metrics")
def get_metrics():
    """Get request metrics."""
    stats = request_monitor.get_stats()
    return {
        "status": "ok",
        "metrics": stats
    }


@app.post("/explore", response_model=SignalResponse)
async def run_exploration(request: ExploreRequest):
    if request.session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    with trace_context("phase1-signal-detection", session_id=request.session_id, tags=["phase1"]) as trace:
        progress_store.set_phase(request.session_id, "explore", "Starting signal detection...")
        progress_store.add_log(request.session_id, "Starting exploration phase")
        
        try:
            conn = duckdb_connector.connections.get(request.session_id)
            if not conn:
                duckdb_connector.load_session(request.session_id)
                conn = duckdb_connector.connections.get(request.session_id)

            df = conn.execute("SELECT * FROM clean_dataset").fetchdf()
            progress_store.add_log(request.session_id, f"Loaded {len(df)} rows from database")
            
            if trace:
                trace.input = f"Loaded {len(df)} rows from database"

            # Generate data summary
            schema = _session_schemas.get(request.session_id, {})
            inferred_types = schema.get("inferred_types", {})
        
            numeric_cols = [c for c, t in inferred_types.items() if t in ["int", "bigint", "float", "double"]]
            categorical_cols = [c for c, t in inferred_types.items() if t in ["string", "varchar"]]
            date_cols = [c for c, t in inferred_types.items() if t in ["datetime", "date", "timestamp"]]
            
            data_summary = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "numeric_columns": len(numeric_cols),
                "categorical_columns": len(categorical_cols),
                "date_columns": len(date_cols),
                "column_list": list(df.columns),
                "numeric_list": numeric_cols,
                "categorical_list": categorical_cols,
                "date_list": date_cols
            }
            
            # Get quality info
            quality = _session_quality.get(request.session_id, {})
            if quality:
                data_summary["missingness"] = quality.get("missingness", {})
                data_summary["outliers"] = quality.get("outliers", {})
                data_summary["total_rows_raw"] = quality.get("total_rows", 0)
                data_summary["valid_rows"] = quality.get("valid_rows", 0)
                data_summary["quarantined_rows"] = quality.get("quarantined_rows", 0)

            progress_store.add_log(request.session_id, "Scanning for correlations...")
            signals = signal_orchestrator.run_exploration(request.session_id, df)

            # Generate signal context/insights for each signal
            signal_insights = []
            for s in signals:
                s_type = s.get("type", "")
                if s_type == "correlation":
                    pair = s.get("pair", [])
                    strength = s.get("strength", 0)
                    s["context"] = f"Found strong positive relationship between {pair[0]} and {pair[1]} (r={strength}). These variables move together."
                    signal_insights.append(f"Correlation: {pair[0]} and {pair[1]} have r={strength} correlation")
                elif s_type == "anomaly":
                    feat = s.get("feature", "")
                    count = s.get("anomaly_count", 0)
                    s["context"] = f"Detected {count} unusual values in {feat}. These deviate significantly from the normal pattern."
                    signal_insights.append(f"Anomaly: {count} outliers found in {feat}")
                elif s_type == "cluster":
                    clusters = s.get("clusters", 0)
                    features = s.get("feature_group", [])
                    s["context"] = f"Data naturally divides into {clusters} distinct behavioral groups based on {', '.join(features)}. Each group represents different user behavior patterns."
                    signal_insights.append(f"Cluster: {clusters} user segments identified")
                elif s_type == "distribution_shift":
                    feat = s.get("feature", "")
                    early = s.get("early_mean", 0)
                    late = s.get("late_mean", 0)
                    s["context"] = f"{feat} distribution changed significantly over time. Early period mean: {early:.1f}, late period mean: {late:.1f}. This indicates a temporal shift."
                    signal_insights.append(f"Distribution shift: {feat} changed over time")
                elif s_type == "trend_break":
                    feat = s.get("feature", "")
                    ts = s.get("timestamp", "")
                    mag = s.get("change_magnitude", 0)
                    s["context"] = f"{feat} shows a significant change around position {ts} with magnitude {mag:.2f}. This represents a structural break in the trend."
                    signal_insights.append(f"Trend break: {feat} changed at position {ts}")

            # Count signals by type
            correlations = len([s for s in signals if s.get("type") == "correlation"])
            anomalies = len([s for s in signals if s.get("type") == "anomaly"])
            clusters = len([s for s in signals if s.get("type") == "cluster"])
            shifts = len([s for s in signals if s.get("type") == "distribution_shift"])
            trends = len([s for s in signals if s.get("type") == "trend_break"])
            
            progress_store.add_log(request.session_id, f"Found {correlations} correlations")
            progress_store.add_log(request.session_id, f"Found {anomalies} anomalies")
            progress_store.add_log(request.session_id, f"Found {clusters} clusters")
            progress_store.add_log(request.session_id, f"Found {shifts} distribution shifts")
            progress_store.add_log(request.session_id, f"Found {trends} trend breaks")
            progress_store.add_log(request.session_id, f"Exploration complete: {len(signals)} signals detected")
            
            progress_store.set_stats(request.session_id, {
                "signals": len(signals),
                "hypotheses": 0,
                "accepted": 0,
                "rejected": 0,
                "plots": 0,
                "insights": 0
            })

            if trace:
                trace.output = str({"signal_count": len(signals), "correlations": correlations, "anomalies": anomalies})

            return SignalResponse(
                session_id=request.session_id,
                signal_count=len(signals),
                signals=signals,
                data_summary=data_summary,
                signal_insights=signal_insights,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/{session_id}", response_model=SignalResponse)
def get_signals(session_id: str):
    signals = signal_orchestrator.store.load_signals(session_id)
    if signals is None:
        raise HTTPException(status_code=404, detail="Signals not found. Run /explore first.")

    return SignalResponse(
        session_id=session_id,
        signal_count=len(signals),
        signals=signals,
    )


@app.post("/hypotheses", response_model=HypothesisGraphResponse)
async def generate_hypotheses(request: HypothesisRequest):
    if request.session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    signals = signal_orchestrator.store.load_signals(request.session_id)
    if signals is None:
        raise HTTPException(status_code=404, detail="Signals not found. Run /explore first.")

    schema = _session_schemas[request.session_id]

    with trace_context("phase2-hypothesis-generation", session_id=request.session_id, tags=["phase2"]) as trace:
        progress_store.set_phase(request.session_id, "hypotheses", "Building hypothesis graph...")
        progress_store.add_log(request.session_id, "✓ Starting hypothesis generation")
        progress_store.add_log(request.session_id, f"→ Loading {len(signals)} signals from Phase 1")

        try:
            progress_store.add_log(request.session_id, "→ Creating hypothesis templates...")
            graph = hypothesis_orchestrator.build_hypotheses(
                request.session_id,
                signals,
                schema
            )

            if trace:
                trace.input = f"Built {len(graph.get('nodes', []))} hypothesis nodes"

            # Count by type
            # Add rationale to each hypothesis
            for node in graph.get("nodes", []):
                node_id = node.get("id", "")
                source_signals = node.get("source_signals", [])
                h_type = node.get("type", "")
                
                if h_type == "causal":
                    if "correlation" in str(source_signals):
                        node["rationale"] = "Generated because a strong correlation was detected between variables. This suggests a potential causal relationship worth testing."
                    elif "anomaly" in str(source_signals):
                        node["rationale"] = "Generated because unusual patterns (outliers) were detected. These may indicate external factors or data quality issues."
                elif h_type == "segment-based":
                    node["rationale"] = "Generated because clustering analysis found distinct groups in the data. Different segments may have different behaviors."
                elif h_type == "temporal":
                    if "distribution_shift" in str(source_signals):
                        node["rationale"] = "Generated because distribution shift was detected between time periods. The data distribution changed significantly."
                    elif "trend_break" in str(source_signals):
                        node["rationale"] = "Generated because a structural break was detected in the trend. The pattern changed at a specific point."
                elif h_type == "data-quality":
                    node["rationale"] = "Generated as a fallback to ensure hypothesis diversity. May indicate signals are artifacts of data collection."

            causal = len([n for n in graph["nodes"] if n.get("type") == "causal"])
            segment = len([n for n in graph["nodes"] if n.get("type") == "segment-based"])
            temporal = len([n for n in graph["nodes"] if n.get("type") == "temporal"])
            dq = len([n for n in graph["nodes"] if n.get("type") == "data-quality"])
            
            progress_store.add_log(request.session_id, f"Generated {causal} causal hypotheses")
            progress_store.add_log(request.session_id, f"Generated {segment} segment-based hypotheses")
            progress_store.add_log(request.session_id, f"Generated {temporal} temporal hypotheses")
            progress_store.add_log(request.session_id, f"Generated {dq} data-quality hypotheses")
            progress_store.add_log(request.session_id, f"Hypothesis graph built: {len(graph['nodes'])} nodes, {len(graph['edges'])} edges")
            
            progress_store.set_stats(request.session_id, {
                "signals": len(signals),
                "hypotheses": len(graph["nodes"]),
                "accepted": 0,
                "rejected": 0,
                "plots": 0,
                "insights": 0
            })

            return HypothesisGraphResponse(
                session_id=request.session_id,
                node_count=len(graph["nodes"]),
                edge_count=len(graph["edges"]),
                execution_order=graph["execution_order"],
                nodes=graph["nodes"],
                edges=graph["edges"],
            )
        except Exception as e:
            if trace:
                trace.output = f"error: {str(e)}"
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/hypotheses/{session_id}", response_model=HypothesisGraphResponse)
def get_hypotheses(session_id: str):
    graph = hypothesis_orchestrator.store.load_graph(session_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Hypotheses not found. Run /hypotheses first.")

    gh = graph.get("hypothesis_graph", {})
    nodes = gh.get("nodes", [])
    edges = gh.get("edges", [])

    return HypothesisGraphResponse(
        session_id=session_id,
        node_count=len(nodes),
        edge_count=len(edges),
        execution_order=graph.get("execution_order", []),
        nodes=nodes,
        edges=edges,
    )


@app.get("/first_screen/{session_id}")
def get_first_screen(session_id: str):
    # Return Phase4 insights/plots if available, else progress status
    insights = phase4_orchestrator.store.load_insights(session_id)
    if insights:
        return {"status": "ready", "first_screen": insights}

    progress = progress_store.get_progress(session_id)
    return {"status": progress.get("phase", "idle"), "progress": progress}


@app.get("/progress/{session_id}")
def get_progress(session_id: str):
    return progress_store.get_progress(session_id)


@app.post("/run_pipeline")
def run_pipeline(request: dict):
    session_id = request.get("session_id")
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id required")

    if session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    # run pipeline synchronously (caller expects immediate completion)
    try:
        progress_store.set_phase(session_id, "explore", "Starting signal detection")
        conn = duckdb_connector.connections.get(session_id)
        if not conn:
            duckdb_connector.load_session(session_id)
            conn = duckdb_connector.connections.get(session_id)
        import pandas as pd
        df = conn.execute("SELECT * FROM clean_dataset").fetchdf()
        signals = signal_orchestrator.run_exploration(session_id, df)

        progress_store.set_phase(session_id, "hypotheses", "Building hypothesis graph")
        schema = _session_schemas.get(session_id, {})
        hypothesis_graph = hypothesis_orchestrator.build_hypotheses(session_id, signals, schema)

        progress_store.set_phase(session_id, "execute", "Executing hypotheses")
        execution_result = execution_engine.run(session_id, hypothesis_graph)
        execution_store.save_execution(session_id, execution_result)

        progress_store.set_phase(session_id, "phase4", "Generating insights and plots")
        phase4_result = phase4_orchestrator.run(session_id, execution_result, hypothesis_graph, schema)

        progress_store.set_phase(session_id, "completed", "Analysis complete")

        return {"status": "ready", "first_screen": phase4_result}
    except Exception as e:
        progress_store.set_phase(session_id, "error", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/phase4/insights/{session_id}")
def get_phase4_insights(session_id: str):
    insights = phase4_orchestrator.store.load_insights(session_id)
    if not insights:
        raise HTTPException(status_code=404, detail="Phase4 insights not found")
    return insights.get("insights", [])


@app.get("/phase4/plots/{session_id}")
def get_phase4_plots(session_id: str):
    insights = phase4_orchestrator.store.load_insights(session_id)
    if not insights:
        raise HTTPException(status_code=404, detail="Phase4 insights not found")
    return insights.get("plots", [])


@app.get("/progress/stream/{session_id}")
def progress_stream(session_id: str):
    return EventSourceResponse(progress_generator(session_id))


@app.post("/execution/{session_id}/approve")
def approve_execution(session_id: str):
    result = execution_store.load_execution(session_id)
    if not result:
        raise HTTPException(status_code=404, detail="Execution not found")

    result.setdefault('approval', {})
    result['approval'].update({
        'status': 'approved',
        'by': 'human',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    })

    execution_store.save_execution(session_id, result)
    progress_store.set_phase(session_id, 'approval', 'approved by human')
    return {"status": "approved"}


@app.post("/execution/{session_id}/reject")
def reject_execution(session_id: str, reason: dict = None):
    result = execution_store.load_execution(session_id)
    if not result:
        raise HTTPException(status_code=404, detail="Execution not found")

    result.setdefault('approval', {})
    result['approval'].update({
        'status': 'rejected',
        'by': 'human',
        'reason': (reason or {}).get('reason') if isinstance(reason, dict) else None,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    })

    execution_store.save_execution(session_id, result)
    progress_store.set_phase(session_id, 'approval', 'rejected by human')
    return {"status": "rejected"}


@app.post("/execute", response_model=ExecutionResponse)
async def run_execution(request: ExecuteRequest):
    if request.session_id not in _session_schemas:
        raise HTTPException(status_code=404, detail="Session not found")

    graph = hypothesis_orchestrator.store.load_graph(request.session_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Hypotheses not found. Run /hypotheses first.")

    nodes = graph.get("hypothesis_graph", {}).get("nodes", [])
    
    with trace_context("phase3-execution", session_id=request.session_id, tags=["phase3"]) as trace:
        progress_store.set_phase(request.session_id, "execute", "Testing hypotheses...")
        progress_store.add_log(request.session_id, "Starting hypothesis execution")
        progress_store.add_log(request.session_id, f"Loading {len(nodes)} hypotheses from Phase 2")

        try:
            progress_store.add_log(request.session_id, "Running execution engine...")
            result = execution_engine.run(request.session_id, graph)

            execution_store.save_execution(request.session_id, result)
            
            if trace:
                trace.output = str({
                    "accepted_count": len(result.get("accepted_hypotheses", [])),
                    "rejected_count": len(result.get("rejected_hypotheses", [])),
                    "iterations": result.get("iteration", 0)
                })
            
            accepted = result.get("accepted_hypotheses", [])
            rejected = result.get("rejected_hypotheses", [])
            history = result.get("execution_history", [])
            iterations = result.get("iteration", 0)
            approval_required = result.get("approval_required", False)
            
            progress_store.add_log(request.session_id, f"Execution complete after {iterations} iterations")
            
            if accepted:
                progress_store.add_log(request.session_id, f"ACCEPTED: {', '.join(accepted)}")
            if rejected:
                progress_store.add_log(request.session_id, f"REJECTED: {len(rejected)} hypotheses")
                
            # Get current signals count
            signals = signal_orchestrator.store.load_signals(request.session_id) or []
            
            progress_store.set_stats(request.session_id, {
                "signals": len(signals),
                "hypotheses": len(nodes),
                "accepted": len(accepted),
                "rejected": len(rejected),
                "plots": 0,
                "insights": 0
            })

            return ExecutionResponse(
                session_id=request.session_id,
                accepted_hypotheses=accepted,
                rejected_hypotheses=rejected,
                execution_history=history,
                insights=result.get("insights", []),
                iteration=iterations,
                status=result.get("status", "completed"),
                approval_required=approval_required,
            )
        except Exception as e:
            import traceback
            error_detail = f"{str(e)}\n{traceback.format_exc()}"
            if trace:
                trace.output = f"error: {str(e)}"
            raise HTTPException(status_code=500, detail=error_detail)


@app.get("/execution/{session_id}", response_model=ExecutionResponse)
def get_execution(session_id: str):
    result = execution_store.load_execution(session_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Execution not found. Run /execute first.")

    return ExecutionResponse(
        session_id=session_id,
        accepted_hypotheses=result.get("accepted_hypotheses", []),
        rejected_hypotheses=result.get("rejected_hypotheses", []),
        execution_history=result.get("execution_history", []),
        insights=result.get("insights", []),
        iteration=result.get("iteration", 0),
        status=result.get("status", "completed"),
    )


@app.get("/results/{session_id}")
def get_results(session_id: str):
    result = execution_store.load_execution(session_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Execution not found. Run /execute first.")

    return {
        "session_id": session_id,
        "accepted_hypotheses": result.get("accepted_hypotheses", []),
        "rejected_hypotheses": result.get("rejected_hypotheses", []),
        "execution_history": result.get("execution_history", []),
    }


@app.post("/phase4/run", response_model=Phase4Response)
async def run_phase4(request: Phase4Request):
    execution_result = execution_store.load_execution(request.session_id)
    if execution_result is None:
        raise HTTPException(status_code=404, detail="Execution not found. Run /execute first.")

    hypothesis_graph = hypothesis_orchestrator.store.load_graph(request.session_id)
    if hypothesis_graph is None:
        raise HTTPException(status_code=404, detail="Hypotheses not found.")

    schema = _session_schemas.get(request.session_id, {})

    with trace_context("phase4-insights-plots", session_id=request.session_id, tags=["phase4"]) as trace:
        progress_store.set_phase(request.session_id, "insights", "Generating insights and plots...")
        progress_store.add_log(request.session_id, "Starting Phase 4: Evidence Synthesis")
        
        # Check for inline insights from Phase 3
        inline_insights = execution_result.get("insights", [])
        if inline_insights:
            progress_store.add_log(request.session_id, f"Using {len(inline_insights)} inline insights from execution")
        
        progress_store.add_log(request.session_id, "Generating plots from executed hypotheses")

        try:
            # Get config for plot generation
            min_plots_per_hyp = _get_env_int("MIN_PLOTS_PER_HYPOTHESIS", 2)
            max_plots_per_hyp = _get_env_int("MAX_PLOTS_PER_HYPOTHESIS", 3)
            enable_plots = _get_env_bool("ENABLE_PLOT_GENERATION", True)
            
            # Generate plots using proper Phase4Orchestrator components
            plots = []
            if enable_plots and inline_insights:
                # Create Phase4Orchestrator components locally for better control
                from app.phase4 import PlotTypeSelector, PlotCompositionEngine, MultiPlotPlanner, PlotValidator, PlotRenderer
                
                type_selector = PlotTypeSelector()
                composer = PlotCompositionEngine()
                planner = MultiPlotPlanner()
                validator = PlotValidator()
                renderer = PlotRenderer(DATA_DIR)
                
                for ins in inline_insights:
                    hyp_id = ins.get("hypothesis_id", "")
                    h_type = ins.get("type", "unknown")
                    
                    # Get hypothesis details from graph
                    nodes = hypothesis_graph.get("hypothesis_graph", {}).get("nodes", [])
                    hyp = next((n for n in nodes if n.get("id") == hyp_id), None)
                    
                    if not hyp:
                        continue
                    
                    # Plan multiple plots for this hypothesis
                    plot_plans = planner.plan(hyp, schema)
                    
                    # Limit to min/max config
                    plot_plans = plot_plans[:max_plots_per_hyp]
                    if len(plot_plans) < min_plots_per_hyp:
                        # Add default overview if not enough
                        while len(plot_plans) < min_plots_per_hyp:
                            plot_plans.append({"type": "overview", "focus": "overall pattern"})
                    
                    for plan_idx, plan in enumerate(plot_plans):
                        # Get plot type based on hypothesis type and plan
                        ptype = type_selector.select(h_type)
                        
                        # Override based on plan type
                        if plan.get("type") == "time_series":
                            ptype = "line"
                        elif plan.get("type") == "segment_comparison":
                            ptype = "bar"
                        elif plan.get("type") == "overview":
                            # Choose best type for the data
                            if h_type in ("temporal", "trend"):
                                ptype = "line"
                            elif h_type in ("segment-based", "segment"):
                                ptype = "bar"
                            elif h_type in ("causal", "correlation"):
                                ptype = "scatter"
                            elif h_type == "anomaly":
                                ptype = "boxplot"
                        
                        # Create plot spec
                        plot_spec = composer.compose(hyp, schema)
                        plot_spec["type"] = ptype
                        
                        # Validate
                        valid, msg = validator.validate(plot_spec, schema)
                        if not valid:
                            # Try fallback with simpler spec
                            plot_spec = {"x": list(schema.get("columns", []))[0] if schema.get("columns") else None, 
                                        "y": list(schema.get("columns", []))[1] if len(schema.get("columns", [])) > 1 else None,
                                        "type": ptype}
                            valid, msg = validator.validate(plot_spec, schema)
                        
                        if valid:
                            # Render the plot
                            rendered = renderer.render(plot_spec, f"{hyp_id}_{plan_idx}", request.session_id, schema)
                            if rendered:
                                plots.append(rendered)
                            else:
                                # Fallback: add placeholder if rendering failed
                                plots.append({
                                    "plot_id": f"p_{hyp_id}_{plan_idx}",
                                    "type": ptype,
                                    "title": f"{h_type}: {plan.get('focus', 'Analysis')}",
                                    "linked_hypothesis": hyp_id,
                                    "format": "placeholder",
                                    "error": "Rendering failed, showing summary instead"
                                })
                        else:
                            # Skip invalid plots but continue
                            continue
            
            # If no plots generated from insights, create from signals as fallback
            if not plots:
                signals = signal_orchestrator.store.load_signals(request.session_id) or []
                
                # Use actual PlotRenderer
                from app.phase4 import PlotRenderer
                renderer = PlotRenderer(DATA_DIR)
                
                # Get valid columns from schema
                columns = schema.get('columns', [])
                numeric_cols = [c for c in columns if schema.get('inferred_types', {}).get(c) in ('int', 'float', 'double')]
                
                if numeric_cols and len(numeric_cols) >= 2:
                    for sig_idx, sig in enumerate(signals[:max_plots_per_hyp]):
                        sig_type = sig.get("type", "unknown")
                        ptype = "bar"
                        if sig_type == "correlation":
                            ptype = "scatter"
                        elif sig_type == "trend_break":
                            ptype = "line"
                        elif sig_type == "cluster":
                            ptype = "bar"
                        
                        # Create actual plot spec
                        x_col = numeric_cols[0] if len(numeric_cols) > 0 else columns[0]
                        y_col = numeric_cols[1] if len(numeric_cols) > 1 else columns[1]
                        
                        plot_spec = {"x": x_col, "y": y_col, "type": ptype}
                        
                        # Try to render
                        rendered = renderer.render(plot_spec, f"signal_{sig_idx}", request.session_id, schema)
                        
                        if rendered:
                            rendered["title"] = f"Signal: {sig_type} - {sig.get('pair', sig.get('feature', 'analysis'))}"
                            plots.append(rendered)
                        else:
                            # Fallback to placeholder
                            plots.append({
                                "plot_id": f"p_signal_{sig_idx}",
                                "type": ptype,
                                "title": f"Signal: {sig_type} - {sig.get('pair', sig.get('feature', 'analysis'))}",
                                "linked_signal": sig.get("type"),
                                "format": "placeholder"
                            })
            
            progress_store.add_log(request.session_id, f"Generated {len(plots)} plots")
            progress_store.add_log(request.session_id, "Phase 4 complete")
            
            # Get current counts
            signals = signal_orchestrator.store.load_signals(request.session_id) or []
            
            progress_store.set_stats(request.session_id, {
                "signals": len(signals),
                "hypotheses": len(hypothesis_graph.get("nodes", [])),
                "accepted": len(execution_result.get("accepted_hypotheses", [])),
                "rejected": len(execution_result.get("rejected_hypotheses", [])),
                "plots": len(plots),
                "insights": len(inline_insights)
            })

            if trace:
                trace.output = str({"insights": len(inline_insights), "plots": len(plots)})

            # Save to store for chat interface
            if inline_insights or plots:
                from app.phase4 import InsightStore
                insight_store = InsightStore(DATA_DIR)
                insight_store.save_insights(request.session_id, inline_insights, plots)

            return Phase4Response(
                session_id=request.session_id,
                insight_count=len(inline_insights),
                plot_count=len(plots),
                insights=inline_insights,
                plots=plots,
            )
        except Exception as e:
            if trace:
                trace.output = f"error: {str(e)}"
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/phase4/insights/{session_id}")
def get_phase4_insights(session_id: str):
    data = phase4_orchestrator.store.load_insights(session_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Phase 4 not run. Run /phase4/run first.")

    return {
        "session_id": session_id,
        "insights": data.get("insights", []),
        "plot_count": len(data.get("plots", []))
    }


@app.get("/phase4/plots/{session_id}")
def get_phase4_plots(session_id: str):
    data = phase4_orchestrator.store.load_insights(session_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Phase 4 not run. Run /phase4/run first.")

    plots = data.get("plots", [])
    for p in plots:
        p.pop("data", None)

    return {
        "session_id": session_id,
        "plots": plots
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    with trace_context("chat-query", session_id=request.session_id, tags=["chat"]) as trace:
        try:
            result = chat_interface.query(request.session_id, request.query)
            
            if trace:
                trace.input = request.query[:200]
                trace.output = result.get("answer", "")[:200]
            
            return ChatResponse(
                answer=result.get("answer", ""),
                confidence=result.get("confidence", 0),
                insights=result.get("insights", []),
                plots=result.get("plots", []),
            )
        except Exception as e:
            if trace:
                trace.output = f"error: {str(e)}"
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/detect_question/{session_id}")
def detect_question_type_endpoint(query: str):
    """Detect what type of question is being asked."""
    qtype = detect_question_type(query)
    plan = build_execution_plan(qtype, query)
    return {
        "query": query,
        "question_type": qtype,
        "phases_needed": plan["phases"],
        "narrative_style": plan["narrative_style"],
        "plots_needed": plan["plots_needed"]
    }


@app.post("/auto_analyze")
async def auto_analyze(request: ChatRequest):
    """Automatically detect question, run needed phases, and answer."""
    
    qtype = detect_question_type(request.query)
    plan = build_execution_plan(qtype, request.query)
    
    executed = []
    
    try:
        # Phase 0: Ensure data exists
        if request.session_id not in _session_schemas:
            raise HTTPException(status_code=404, detail="Session not found. Upload CSV first.")
        executed.append(0)
        
        # Check and run each needed phase
        if 1 in plan["phases"]:
            signals = signal_orchestrator.store.load_signals(request.session_id)
            if not signals:
                signal_orchestrator.run_exploration(
                    request.session_id,
                    duckdb_connector.connections.get(request.session_id).execute("SELECT * FROM clean_dataset").fetchdf()
                )
                executed.append(1)
        
        if 2 in plan["phases"]:
            graph = hypothesis_orchestrator.store.load_graph(request.session_id)
            if not graph:
                sigs = signal_orchestrator.store.load_signals(request.session_id)
                sch = _session_schemas.get(request.session_id, {})
                hypothesis_orchestrator.build_hypotheses(request.session_id, sigs, sch)
                executed.append(2)
        
        if 3 in plan["phases"]:
            exec_result = execution_store.load_execution(request.session_id)
            if not exec_result:
                graph = hypothesis_orchestrator.store.load_graph(request.session_id)
                if graph:
                    result = execution_engine.run(request.session_id, graph)
                    execution_store.save_execution(request.session_id, result)
                    executed.append(3)
        
        insights = phase4_orchestrator.store.load_insights(request.session_id)
        plots_data = phase4_orchestrator.store.load_insights(request.session_id)
        execution_result = execution_store.load_execution(request.session_id)
        
        insight_list = insights.get("insights", []) if insights else []
        plot_list = plots_data.get("plots", []) if plots_data else []
        
        # If no insights from phase4, try to get from signals as fallback
        if not insight_list:
            signals = signal_orchestrator.store.load_signals(request.session_id)
            if signals:
                for sig in signals[:5]:
                    insight_list.append({
                        "id": sig.get("type", "signal"),
                        "insight": sig.get("context", f"Found {sig.get('type')} in data"),
                        "confidence": sig.get("confidence", 0.5),
                        "type": sig.get("type", "signal"),
                        "hypothesis_id": sig.get("type", "")
                    })
        
        # Calculate average confidence
        avg_confidence = sum(i.get("confidence", 0) for i in insight_list) / max(len(insight_list), 1) if insight_list else 0
        
        # Use GROQ for intelligent response if available
        answer = ""
        if intelligent_router.groq_client and insight_list:
            from app.chat_router import generate_type_aware_prompt
            prompt = generate_type_aware_prompt(
                request.query,
                qtype,
                insight_list,
                plot_list,
                execution_result or {}
            )
            answer = intelligent_router._call_groq(prompt)
        elif insight_list:
            # Generate simple answer from insights
            answer = f"Based on my analysis of your data: "
            for i, ins in enumerate(insight_list[:3]):
                answer += f"\n{i+1}. {ins.get('insight', 'Found insight')}"
        else:
            answer = "I analyzed your data but couldn't find specific patterns. Try uploading a larger dataset or asking a more specific question."
        
        return {
            "question_type": qtype,
            "executed_phases": executed,
            "all_phases_needed": plan["phases"],
            "answer": answer,
            "narrative_style": plan["narrative_style"],
            "insights": insight_list,
            "plots": {p.get("plot_id", f"plot_{i}"): p for i, p in enumerate(plot_list)},
            "trace": [{"phase": f"phase_{p}", "status": "completed" if p in executed else "pending"} for p in [0, 1, 2, 3, 4]],
            "insights_count": len(insight_list),
            "plots_count": len(plot_list),
            "confidence": avg_confidence,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/progress/stream/{session_id}")
async def progress_stream(session_id: str):
    """Server-Sent Events endpoint for real-time progress updates"""
    async def event_generator():
        last_log_count = 0
        while True:
            progress = progress_store.get_progress(session_id)
            
            current_log_count = len(progress.get("logs", []))
            if current_log_count != last_log_count:
                yield {"event": "progress", "data": json.dumps(progress)}
                last_log_count = current_log_count
            else:
                yield {"event": "ping", "data": json.dumps({"phase": progress.get("phase", "idle")})}
            
            await asyncio.sleep(0.5)
    
    return EventSourceResponse(event_generator())


@app.get("/progress/{session_id}")
def get_progress(session_id: str):
    """Get current progress status"""
    return progress_store.get_progress(session_id)


@app.post("/run_pipeline")
def run_full_pipeline(request: dict):
    """
    Run complete analysis pipeline in a single call.
    Chains: explore → hypotheses → execute → phase4
    Returns: insights, plots, trace, stats
    """
    try:
        session_id = request.get("session_id", "")
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        import pandas as pd
        
        # Check if session exists in memory, otherwise try to load from disk
        if session_id not in _session_schemas:
            session_path = os.path.join(DATA_DIR, session_id, "clean.csv")
            if os.path.exists(session_path):
                try:
                    duckdb_connector.load_session(session_id)
                    conn = duckdb_connector.connections.get(session_id)
                    df = conn.execute("SELECT * FROM clean_dataset").fetchdf()
                    _session_schemas[session_id] = {"columns": list(df.columns), "dtypes": {c: str(d) for c, d in df.dtypes.items()}}
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Failed to load session from disk: {str(e)}")
            else:
                raise HTTPException(status_code=404, detail="Session not found. Upload CSV first.")
        
        logger.info(f"Starting run_pipeline for session {session_id}")
        
        result = {
            "session_id": session_id,
            "insights": [],
            "plots": {},
            "trace": [],
            "dataset_meta": {},
            "accepted_count": 0,
            "rejected_count": 0,
            "status": "pending"
        }
        
        # Ensure connection exists
        if session_id not in duckdb_connector.connections:
            duckdb_connector.load_session(session_id)
        
        df = duckdb_connector.connections.get(session_id).execute("SELECT * FROM clean_dataset").fetchdf()
        
        # Step 1: Explore (Signal Detection)
        progress_store.set_phase(session_id, "explore", "Running signal detection...")
        signals = signal_orchestrator.store.load_signals(session_id)
        if not signals:
            signals = signal_orchestrator.run_exploration(session_id, df)
        result["trace"].append({"phase": "explore", "signals": len(signals)})
        
        # Step 2: Hypotheses
        progress_store.set_phase(session_id, "hypotheses", "Generating hypotheses...")
        graph = hypothesis_orchestrator.store.load_graph(session_id)
        if not graph:
            schema = _session_schemas.get(session_id, {})
            graph = hypothesis_orchestrator.build_hypotheses(session_id, signals, schema)
        result["trace"].append({"phase": "hypotheses", "nodes": len(graph.get("nodes", []))})
        
        # Step 3: Execute
        progress_store.set_phase(session_id, "execute", "Testing hypotheses...")
        exec_result = execution_store.load_execution(session_id)
        if not exec_result:
            exec_result = execution_engine.run(session_id, graph)
            execution_store.save_execution(session_id, exec_result)
        
        accepted = exec_result.get("accepted_hypotheses", [])
        rejected = exec_result.get("rejected_hypotheses", [])
        
        # Generate fallback insights directly from signals for display
        if len(result["insights"]) == 0:
            logger.warning("No accepted hypotheses, creating insights from signals directly")
            # Graph uses 'nodes' key directly
            nodes = graph.get("nodes", [])
            if not nodes:
                nodes = graph.get("hypothesis_graph", {}).get("nodes", [])
            logger.info(f"Found {len(nodes)} nodes in graph")
            for i, node in enumerate(nodes[:5]):
                if node.get("description"):
                    insight = {
                        "id": f"insight_{i}",  # Add unique ID for frontend
                        "insight": node.get("description", ""),
                        "confidence": node.get("signal_support", 0.5) or 0.5,
                        "hypothesis_id": node.get("id", ""),
                        "effect_size": 0.1,
                        "type": node.get("type", "signal"),
                        "generated_at": "signal",
                        "plot_ids": []  # Will be filled by plot generation
                    }
                    result["insights"].append(insight)
                    logger.info(f"Added insight: {insight.get('insight', '')[:50]}")
        
        exec_result["accepted_hypotheses"] = accepted
        exec_result["rejected_hypotheses"] = rejected
        result["accepted_count"] = len(accepted)
        result["rejected_count"] = len(rejected)
        result["trace"].append({
            "phase": "execute", 
            "accepted": len(accepted),
            "rejected": len(rejected)
        })
        
        # Step 4: Phase 4 (Insights + Plots)
        progress_store.set_phase(session_id, "insights", "Generating insights and plots...")
        
        schema = _session_schemas.get(session_id, {})
        
        if len(result["insights"]) == 0:
            logger.info("Running phase4 for insights")
            phase4_result = phase4_orchestrator.run(session_id, exec_result, graph, schema)
            if phase4_result.get("insights"):
                result["insights"] = phase4_result.get("insights", [])
        
        # Generate plots for fallback insights
        if len(result["insights"]) > 0 and not result["plots"]:
            logger.info("Generating plots for fallback insights")
            if result["insights"]:
                # Sample data for plotting
                df_plot = df.sample(min(5000, len(df)), random_state=42) if len(df) > 5000 else df
                schema = _session_schemas.get(session_id, {})
                columns = schema.get("columns", [])
                inferred = schema.get("inferred_types", {})
                
                # Categorize columns by type
                numeric_cols = [c for c in columns if inferred.get(c) in ['int', 'float'] or df_plot[c].dtype in ['int64', 'float64']]
                categorical_cols = [c for c in columns if inferred.get(c) == 'string' and df_plot[c].nunique() < 20]
                
                plot_idx = 0
                max_plots = 6
                
                # Create plot_ids mapping
                plot_ids_list = []
                
                # 1. Histogram for numeric distributions (top numeric)
                for col in numeric_cols[:3]:
                    if plot_idx >= max_plots:
                        break
                    hist_data = df_plot[col].dropna().tolist()[:1000]
                    if hist_data and len(hist_data) > 10:
                        plot_id = f"plot_{plot_idx}"
                        plot_ids_list.append(plot_id)
                        result["plots"][plot_id] = {
                            "plot_id": plot_id,
                            "type": "histogram",
                            "x": col,
                            "title": f"Distribution of {col}",
                            "x": hist_data,  # Flat format for Plotly
                            "marker": {"color": "#3b82f6"}
                        }
                        plot_idx += 1
                
                # 2. Bar chart for categorical (value counts)
                for col in categorical_cols[:2]:
                    if plot_idx >= max_plots:
                        break
                    counts = df_plot[col].value_counts().head(10)
                    if len(counts) > 1:
                        plot_id = f"plot_{plot_idx}"
                        plot_ids_list.append(plot_id)
                        result["plots"][plot_id] = {
                            "plot_id": plot_id,
                            "type": "bar",
                            "x": col,
                            "title": f"{col} distribution",
                            "x": counts.index.tolist(),  # Flat format
                            "y": counts.values.tolist(),
                            "marker": {"color": "#10b981"}
                        }
                        plot_idx += 1
                
                # 3. Scatter for numeric pairs (top correlations)
                if len(numeric_cols) >= 2:
                    for i, x_col in enumerate(numeric_cols[:min(3, len(numeric_cols))]):
                        for y_col in numeric_cols[i+1:i+2]:
                            if plot_idx >= max_plots:
                                break
                            plot_id = f"plot_{plot_idx}"
                            plot_ids_list.append(plot_id)
                            result["plots"][plot_id] = {
                                "plot_id": plot_id,
                                "type": "scatter",
                                "x": x_col,
                                "y": y_col,
                                "title": f"{x_col} vs {y_col}",
                                "x": df_plot[x_col].dropna().tolist()[:300],  # Flat format
                                "y": df_plot[y_col].dropna().tolist()[:300],
                                "marker": {"color": "#f59e0b", "size": 6}
                            }
                            plot_idx += 1
                
                # Link all plots to each insight
                if plot_ids_list:
                    for insight in result["insights"]:
                        insight["plot_ids"] = plot_ids_list[:3]  # Link first 3 plots to each insight
                
                logger.info(f"Generated {plot_idx} plots")
        
        result["status"] = "completed"
        result["trace"].append({
            "phase": "phase4",
            "insights": len(result["insights"]),
            "plots": len(result["plots"])
        })
        
        # Get dataset metadata
        result["dataset_meta"] = {
            "rows": len(df),
            "columns": len(df.columns),
            "quality": {}
        }
        
        logger.info(f"Pipeline complete: {len(result['insights'])} insights, {len(result['plots'])} plots")
        
        # Save insights and plots to Phase4 store for NLQ to access
        try:
            phase4_orchestrator.store.save_insights(session_id, result["insights"], list(result["plots"].values()))
        except Exception as e:
            logger.warning(f"Could not save to phase4 store: {e}")
        
        return result
        
    except Exception as e:
        import traceback
        logger.error(f"Pipeline error: {str(e)}")
        return {
            "session_id": session_id,
            "status": "error",
            "error": str(e),
            "trace": traceback.format_exc(),
            "insights": [],
            "plots": {}
        }


@app.post("/simulate")
def simulate_prediction(request: dict):
    """Run what-if simulation"""
    session_id = request.get("session_id")
    variable = request.get("variable")
    change_pct = request.get("change_percent", 0)
    
    if not session_id or not variable:
        return {"success": False, "error": "session_id and variable required"}
    
    try:
        from app.predictive import FeatureBuilder, ModelEngine, SimulationEngine
        
        fb = FeatureBuilder("data")
        df = fb.load_clean_data(session_id)
        
        if df is None or len(df) < 50:
            return {"success": False, "error": "Insufficient data for prediction"}
        
        numeric_cols = fb.get_numeric_columns(df)
        if variable not in numeric_cols:
            return {"success": False, "error": f"Variable {variable} not found or not numeric"}
        
        X, y = fb.prepare_training_data(df, variable)
        if X is None:
            return {"success": False, "error": "Could not prepare training data"}
        
        me = ModelEngine()
        train_result = me.train_regression(X, y)
        
        if not train_result.get("success"):
            return {"success": False, "error": train_result.get("error", "Training failed")}
        
        se = SimulationEngine(me)
        sim_result = se.simulate_change(X.iloc[0].to_dict(), variable, change_pct)
        
        return {
            "success": True,
            "variable": variable,
            "change_percent": change_pct,
            "predicted_value": sim_result.get("predicted_value"),
            "confidence": train_result.get("confidence", 0),
            "method": "regression_simulation"
        }
        
    except Exception as e:
        trace("simulate_error", {"error": str(e)})
        return {"success": False, "error": str(e)}


@app.on_event("shutdown")
async def shutdown_event():
    from app.langfuse_client import flush
    flush()