# Data Analyst Agent - Detailed Documentation

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Technical Stack](#technical-stack)
3. [Phase-by-Phase Analysis](#phase-by-phase-analysis)
4. [Core Modules](#core-modules)
5. [Data Pipeline](#data-pipeline)
6. [API Endpoints](#api-endpoints)
7. [Security & Reliability](#security--reliability)
8. [Observability](#observability)
9. [Configuration](#configuration)
10. [Database Schema](#database-schema)
11. [Error Handling](#error-handling)

---

## Architecture Overview

The **Data Analyst Agent** is an agentic data reasoning system that transforms raw CSV datasets into validated insights through a structured 4-phase pipeline with explicit hypothesis testing.

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                           DATA ANALYST AGENT PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│   [Data]    Phase 0     Phase 1      Phase 2     Phase 3     Phase 4            │
│   CSV    ─────────► ─────────► ─────────► ─────────► ─────────► ┌──────────────┐     │
│                Ingestion  Signals    Hypotheses  Execution  │ Insights     │     │
│                (Layers)   (Explore)               (Execute) │ + Plots      │     │
│                                                      │    │ + Chat UI    │     │
│                                                      │    └──────────────┘     │
│                                                      │              │            │
│                                                      └──────────────┘          │
│                                                      (Evidence-Backed)         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Separation of Concerns**: Each phase has a single responsibility
2. **No Reasoning Leakage**: Phase 4 only uses evidence from Phase 3
3. **Deterministic Execution**: SQL-based execution, statistical thresholds
4. **Evidence-Backed Insights**: No free-form generation in core pipeline
5. **Traceability**: Answer → Insight → Hypothesis → SQL → Data

---

## Technical Stack

| Component | Technology | Version |
|------------|------------|---------|
| Backend Framework | FastAPI | Latest |
| Database (In-Memory) | DuckDB | Latest |
| Data Processing | Pandas, NumPy | Latest |
| ML/Statistics | Scikit-learn, SciPy | Latest |
| Plotting | Matplotlib | Latest |
| LLM Integration | GROQ SDK | Latest |
| Observability | Langfuse | Latest |
| Rate Limiting | Custom Middleware | N/A |
| Validation | Pydantic | Latest |

### Key Dependencies

```
# Core
fastapi
uvicorn
pydantic
pandas
numpy
duckdb
scikit-learn
scipy
matplotlib

# LLM & Observability
groq
langfuse

# Utilities
python-dotenv
sse-starlette
```

---

## Phase-by-Phase Analysis

### Phase 0: Data Ingestion (Layers)

**File**: `app/layers.py` (274 lines)

The data ingestion layer handles file upload, schema validation, and data cleaning. It consists of:

| Class | Responsibility |
|-------|----------------|
| `FileIntakeLayer` | Session-based file storage with SHA256 checksum |
| `SchemaValidator` | Column normalization, type inference, type enforcement |
| `MissingnessProfiler` | Missing value analysis per column |
| `OutlierScanner` | IQR-based outlier detection |
| `DataFrameMaterializer` | Persist clean/quarantine CSV files |
| `SemanticMapper` | Role assignment (time_axis, metric, dimension, entity_key) |
| `DuckDBConnector` | In-memory DuckDB connection management |

**Processing Flow**:
```
Raw CSV → Upload → Store (raw.csv) → Normalize Columns → Infer Types
     → Enforce Types → Quarantine Invalid → Materialize (clean.csv, quarantine.csv)
     → Load to DuckDB → Queryable Dataset
```

**Key Types Inferred**:
- `date`: YYYY-MM-DD format
- `datetime`: ISO format
- `int`: Integer values
- `float`: Decimal values
- `string`: Default fallback

**Session Storage**:
```
data/{session_id}/
├── raw.csv           # Original uploaded file
├── clean.csv         # Valid rows only
└── quarantine.csv    # Invalid rows (type enforcement failures)
```

---

### Phase 1: Signal Detection (Explore)

**File**: `app/explore.py` (365 lines)

Detects meaningful patterns in the data through multiple analysis methods:

| Module | Method | Output |
|--------|--------|--------|
| `CorrelationEngine` | Pearson correlation (threshold: 0.6) | Linked variable pairs |
| `AnomalyDetector` | Z-score + IQR methods | Outlier counts per feature |
| `ClusteringEngine` | KMeans clustering | Natural groupings |
| `DistributionShiftDetector` | KS-test (early vs late) | Temporal shifts |
| `TrendBreakDetector` | Rolling mean change points | Trend breaks |
| `SignalScorer` | Confidence/strength scoring | Weighted signals |

**Signal Types**:

1. **Correlation**: `{"type": "correlation", "pair": [A, B], "strength": 0.87, "p_value": 0.001}`
2. **Anomaly**: `{"type": "anomaly", "feature": "latency", "anomaly_count": 15, "severity": "medium"}`
3. **Cluster**: `{"type": "cluster", "feature_group": [...], "clusters": 3, "inertia": 142.5}`
4. **Distribution Shift**: `{"type": "distribution_shift", "feature": "revenue", "shift_score": 0.45}`
5. **Trend Break**: `{"type": "trend_break", "feature": "sessions", "change_magnitude": 230}`

**Signal Storage**:
```json
// data/{session_id}/signals.json
{
  "signals": [
    {
      "type": "correlation",
      "pair": ["latency_ms", "sessions"],
      "strength": 0.72,
      "confidence": 0.85,
      // ... additional fields
    }
  ]
}
```

---

### Phase 2: Hypothesis Generation (Hypothesis)

**File**: `app/hypothesis.py` (519 lines)

Generates testable hypotheses from detected signals:

| Component | Function |
|-----------|-----------|
| `HypothesisGenerator` | Creates hypotheses from signals |
| `HypothesisGraphBuilder` | Builds nodes/edges structure |
| `DiversityEnforcer` | Ensures hypothesis type diversity |
| `HypothesisScorer` | Scores by signal support, plausibility |
| `HypothesisExpander` | Creates variants for top hypotheses |
| `GraphStore` | Persists hypothesis graph |

**Hypothesis Types**:

| Type | Description | Example |
|------|-------------|---------|
| `causal` | X influences Y | "revenue influences profit" |
| `segment-based` | Effect varies across groups | "mobile vs desktop conversion" |
| `temporal` | Change over time | "revenue dropped after update" |
| `data-quality` | Signal may be artifact | "outliers are data entry errors" |

**Graph Structure**:
```json
{
  "hypothesis_graph": {
    "nodes": [
      {
        "id": "H_corr_revenue_profit",
        "type": "causal",
        "description": "revenue influences profit with strength 0.85",
        "features": ["revenue", "profit"],
        "segments": [],
        "source_signals": ["correlation_revenue_profit"],
        "expected_observations": [...],
        "falsification_tests": [...],
        "score": 0.75
      }
    ],
    "edges": [
      {"from_node": "H_dq_enforced", "to_node": "H_corr_revenue_profit", "edge_type": "competes_with"}
    ]
  },
  "execution_order": ["H_corr_revenue_profit", "H_dq_enforced", ...]
}
```

---

### Phase 3: Execution Engine (Execute)

**File**: `app/execute.py` (657 lines)

Tests hypotheses through SQL execution and statistical evaluation:

| Component | Function |
|-----------|-----------|
| `QueryPlanner` | Converts hypothesis → SQL plan |
| `QueryValidator` | Validates SQL (SQL Guard integration) |
| `EvidenceEvaluator` | Statistical evaluation of results |
| `DecisionEngine` | Accept/reject/refine decisions |
| `ExecutionEngine` | Main execution orchestrator |
| `ExecutionStore` | Persists execution results |

**Execution Flow**:
```
Select Hypothesis → Plan Query → Validate → Execute → Evaluate → Decide
     ↓                                                           ↓
  Next Iteration ────────────────────────────────────── Complete
```

**Decision Thresholds**:
- **Accept**: confidence >= 0.75
- **Reject**: confidence <= 0.30
- **Refine**: otherwise (create child hypothesis)

**Statistical Metrics**:
- `confidence`: Overall confidence (0-1)
- `effect_size`: Practical significance
- `support`: Evidence strength

**Execution Results**:
```json
{
  "accepted_hypotheses": ["H_corr_revenue_profit"],
  "rejected_hypotheses": ["H_dq_enforced", ...],
  "execution_history": [
    {
      "hypothesis_id": "H_corr_revenue_profit",
      "action": "accept",
      "evidence": {"confidence": 0.82, "effect_size": 0.65},
      "iteration": 1
    }
  ],
  "insights": [
    {
      "insight": "revenue influences profit with strength 0.85...",
      "confidence": 0.82,
      "hypothesis_id": "H_corr_revenue_profit",
      "effect_size": 0.65,
      "type": "causal"
    }
  ]
}
```

---

### Phase 4: Insights & Visualization (Phase4)

**File**: `app/phase4.py` (534 lines)

Generates final insights, plots, and chat responses:

| Component | Function |
|-----------|-----------|
| `EvidenceAggregator` | Aggregates evidence from execution |
| `InsightConstructor` | Constructs natural language insights |
| `ContradictionResolver` | Handles conflicting evidence |
| `PlotTypeSelector` | Maps hypothesis → plot type |
| `PlotCompositionEngine` | Creates plot specs |
| `PlotValidator` | Validates plot specs |
| `PlotRenderer` | Renders matplotlib → Base64 PNG |
| `InsightStore` | Persists insights + plots |
| `ChatInterface` | GROQ-powered chat |

**Plot Types**:
| Hypothesis Type | Plot Type |
|-----------------|-----------|
| temporal/trend | line |
| segment-based | bar |
| correlation | scatter |
| anomaly | boxplot |
| distribution_shift | histogram |

**Insight Construction**:
```python
if confidence >= MIN_CONFIDENCE_TO_REPORT (0.6):
    if type == "causal":
        insight = f"{claim} Supported by evidence with confidence {conf:.2f}."
    elif type == "segment-based":
        insight = f"Behavioral differences detected. {claim} Confidence: {conf:.2f}."
    # ... more types
```

**Chat Interface**:
- Uses only retrieved insights (no new reasoning)
- Type-aware prompt generation
- Keyword matching for relevant insights

---

## Core Modules

### `app/main.py` (1240 lines)

FastAPI application entry point. Defines all endpoints and orchestrates phases.

**Key Endpoints**:
```python
@app.post("/upload_csv")           # Phase 0
@app.get("/schema/{session_id}")  # Get schema
@app.get("/data_quality/...")      # Get quality report
@app.post("/query")               # SQL query
@app.post("/explore")              # Phase 1
@app.get("/signals/{session_id}") # Get signals
@app.post("/hypotheses")           # Phase 2
@app.get("/hypotheses/{session_id}")
@app.post("/execute")              # Phase 3
@app.get("/execution/{session_id}")
@app.post("/phase4/run")            # Phase 4
@app.post("/chat")                 # Chat interface
@app.post("/auto_analyze")         # Auto-detect and answer
@app.get("/progress/stream/{session_id}")  # SSE progress
```

### `app/models.py` (115 lines)

Pydantic models for request/response validation:

```python
class UploadResponse(BaseModel):
    session_id: str
    checksum: str
    file_size: int
    message: str

class SchemaInfo(BaseModel):
    columns: list[str]
    inferred_types: dict[str, str]

class SignalResponse(BaseModel):
    session_id: str
    signal_count: int
    signals: list[dict]
    data_summary: dict

class HypothesisGraphResponse(BaseModel):
    session_id: str
    node_count: int
    edge_count: int
    execution_order: list[str]
    nodes: list[dict]
    edges: list[dict]

class ExecutionResponse(BaseModel):
    session_id: str
    accepted_hypotheses: list
    rejected_hypotheses: list
    execution_history: list
    insights: list
    iteration: int
    status: str
```

### `app/chat_router.py` (298 lines)

Intelligent chat routing with question type detection:

| Question Type | Phases Needed | Plots Needed |
|---------------|-------------|--------------|
| DESCRIPTIVE | [0] | bar, histogram |
| TREND | [0,1,2,3] | line, time_series |
| COMPARATIVE | [0,1,2,3] | bar, grouped_bar |
| CAUSAL | [0,1,2,3] | scatter, boxplot |
| ANOMALY | [0,1,2,3] | boxplot, scatter |
| CORRELATION | [0,1,2,3] | scatter, heatmap |
| SEGMENT_DISCOVERY | [0,1,2,3] | cluster, bar |
| CHANGE_DETECTION | [0,1,2,3] | line, histogram |

### `app/progress.py` (65 lines)

Real-time progress tracking with SSE:

```python
class ProgressStore:
    def set_phase(session_id, phase, message)
    def add_log(session_id, log)
    def set_stats(session_id, stats)
    def get_progress(session_id) -> dict
```

---

## Data Pipeline

### Complete Execution Pipeline

```python
# 1. Upload CSV → Phase 0
r = client.post("/upload_csv", files={"file": open("data.csv", "rb")})
session_id = r.json()["session_id"]

# 2. Explore → Phase 1
client.post("/explore", json={"session_id": session_id})

# 3. Hypotheses → Phase 2
client.post("/hypotheses", json={"session_id": session_id})

# 4. Execute → Phase 3
client.post("/execute", json={"session_id": session_id})

# 5. Phase 4 → Insights + Plots
client.post("/phase4/run", json={"session_id": session_id})

# 6. Chat
client.post("/chat", json={
    "session_id": session_id,
    "query": "Why did revenue drop?"
})
```

### Alternative: Auto-Analyze

```python
# Single endpoint with automatic phase detection
client.post("/auto_analyze", json={
    "session_id": session_id,
    "query": "Why did revenue drop?"
})
# Automatically runs needed phases based on question type
```

---

## API Endpoints

### Phase 0: Data Ingestion

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/upload_csv` | POST | Upload CSV, returns session_id |
| `/schema/{session_id}` | GET | Get inferred schema |
| `/data_quality/{session_id}` | GET | Get missingness/outliers |
| `/query` | POST | Execute SQL query |
| `/health` | GET | Health check |
| `/metrics` | GET | Request metrics |

### Phase 1: Signal Detection

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/explore` | POST | Run signal detection |
| `/signals/{session_id}` | GET | Get detected signals |

### Phase 2: Hypothesis Generation

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/hypotheses` | POST | Generate hypothesis graph |
| `/hypotheses/{session_id}` | GET | Get hypothesis graph |

### Phase 3: Execution

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/execute` | POST | Run hypothesis execution |
| `/execution/{session_id}` | GET | Get execution results |
| `/results/{session_id}` | GET | Get execution summary |

### Phase 4: Insights & Visualization

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/phase4/run` | POST | Generate insights + plots |
| `/phase4/insights/{session_id}` | GET | Get insights |
| `/phase4/plots/{session_id}` | GET | Get plots |

### Chat Interface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/chat` | POST | Chat with GROQ |
| `/auto_analyze` | POST | Auto-detect + answer |
| `/detect_question/{session_id}` | GET | Detect question type |

### Progress & Pipeline

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/progress/stream/{session_id}` | GET | SSE progress stream |
| `/progress/{session_id}` | GET | Get progress |
| `/run_pipeline` | POST | Run full pipeline |

---

## Security & Reliability

### SQL Guard (`app/security/sql_guard.py`)

Security rules for SQL queries:

```python
# Rules:
# - ONLY SELECT allowed (no INSERT, UPDATE, DELETE, DROP, etc.)
# - LIMIT clause required (max 1000 rows)
# - Max query length: 10000 chars
# - Forbidden: load_extension, attach, detach

def validate_sql(query: str) -> bool:
    # Raises SQLSecurityError if invalid
    ...
```

### Threat Model (`app/security/threat_model.py`)

- Agent boundary enforcement
- Plot spec validation (rejects ID columns)
- Chat grounding validation (requires source citations)

### Rate Limiting (`app/monitoring.py`)

```python
class RateLimiter(BaseHTTPMiddleware):
    # Default: 60 requests/minute, 10 burst
    # Returns 429 if exceeded
```

### Circuit Breaker (`app/reliability/retry.py`)

- Opens after 3 consecutive failures
- Resets after 60 seconds
- Prevents cascading failures

---

## Observability

### Logging (`app/monitoring.py`)

```python
# File: logs/app.log (rotating, 10MB max, 5 backups)
# Format: "YYYY-MM-DD HH:MMSS | LEVEL | NAME | MESSAGE"

def setup_logging(log_dir: str, level: str) -> Logger:
    # Creates rotating file handler + console handler
    ...
```

### Langfuse Integration (`app/langfuse_client.py`)

Traced endpoints:
- `/explore` - Phase 1
- `/hypotheses` - Phase 2
- `/execute` - Phase 3
- `/phase4/run` - Phase 4
- `/chat` - Chat queries

```python
# .env configuration
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=https://cloud.langfuse.com
```

---

## Configuration

### Environment Variables

```bash
# Required
GROQ_API_KEY=your_groq_api_key
GROQ_MODEL=llama-3.3-70b-versatile

# Phase 0 & 1
CORRELATION_THRESHOLD=0.6
Z_SCORE_THRESHOLD=3.0
IQR_MULTIPLIER=1.5
MIN_ROWS_FOR_CLUSTERING=50
MAX_CLUSTERS=5

# Phase 2
HYPOTHESIS_MAX_COUNT=10

# Phase 3
MAX_ITERATIONS=5
ACCEPT_THRESHOLD=0.75
REJECT_THRESHOLD=0.30
MIN_CONFIDENCE_TO_REPORT=0.6
CIRCUIT_BREAKER_FAILURES=3
CIRCUIT_BREAKER_TIMEOUT=60

# Phase 4
ENABLE_PLOT_GENERATION=true
MAX_PLOTS_PER_HYPOTHESIS=3

# Observability
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...

# Monitoring
LOG_DIR=logs
LOG_LEVEL=INFO
RATE_LIMIT_RPM=60
RATE_LIMIT_BURST=10
APP_VERSION=1.0.0

# Data
DATA_DIR=data
```

---

## Database Schema

### DuckDB Tables

```sql
-- Clean dataset (created from CSV)
CREATE TABLE clean_dataset AS SELECT * FROM read_csv_auto('path/to/clean.csv');
```

### Phase Output Files

| Phase | File | Key Fields |
|-------|------|------------|
| 0 | `{session}/clean.csv` | Typed columns, valid rows |
| 0 | `{session}/quarantine.csv` | Invalid rows |
| 1 | `{session}/signals.json` | type, confidence, strength |
| 2 | `{session}/hypothesis_graph.json` | nodes, edges, execution_order |
| 3 | `{session}/execution_results.json` | accepted, rejected, history |
| 4 | `{session}/phase4_insights.json` | insights, plots |

---

## Error Handling

### Error Responses

| Scenario | Response |
|----------|----------|
| Session not found | 404: "Session not found. Upload CSV first." |
| Invalid phase data | 404: "Signals not found. Run /explore first." |
| SQL error | 400: str(e) |
| Phase crash | 500: error detail |
| Rate limit exceeded | 429: "Too many requests" |

### Graceful Degradation

| Condition | Behavior |
|-----------|----------|
| No signals found | Return empty signals, continue |
| No valid hypotheses | Return diversity enforcement fallback |
| Conflicting evidence | Accept highest confidence, reject others |
| GROQ unavailable | Return cached insights without LLM |
| Langfuse unavailable | No tracing (no errors) |

---

## Limitations

- **Unstructured data**: Requires typed CSV
- **No forecasting**: Only descriptive analysis
- **Single dataset**: No multi-dataset reasoning
- **GROQ dependency**: Requires API key for natural language chat
- **In-memory database**: Data not persisted across restarts

---

## Running the Application

### Backend

```bash
# Install dependencies
pip install -e .

# Start server
uvicorn app.main:app --reload --port 8015
```

### Frontend

```bash
cd frontend
npm install
npm run dev -- --port 3000
```

### Testing

```bash
# Run evaluation tests
python tests/test_evaluation.py

# Run API tests
python tests/test_api.py

# Run full pipeline test
python tests/test_full_pipeline.py
```

---

## Glossary

| Term | Definition |
|------|------------|
| Session | Isolated analysis context (UUID) |
| Signal | Pattern detected in data |
| Hypothesis | Testable explanation of signal |
| Evidence | SQL query results |
| Insight | Validated conclusion |
| Plot | Visualization of evidence |
| Grounding | Evidence-backed response |
| Circuit Breaker | Failure cascade prevention |
| Rate Limiter | Request throttling |
| Tracing | Observability recording |