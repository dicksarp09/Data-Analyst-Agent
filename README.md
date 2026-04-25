# Data Analyst Agent

An agentic data reasoning system that transforms raw CSV datasets into validated insights through structured hypothesis testing and execution.

---

## Problem Statement

Existing analytics tools fail at:

- **No structured hypothesis testing**: Tools like Tableau/PowerBI show correlations but don't test causal explanations
- **Black-box ML**: Python notebooks produce models but lack traceability from data to conclusions
- **No execution loop**: Business intelligence tools don't actually "test" hypotheses - they just visualize
- **Hallucination**: LLMs generate insights without grounding in actual data evidence

This system improves by:
- Explicit hypothesis graphs with test/reject/accept decisions
- Full traceability: answer → insight → hypothesis → SQL → data
- Evidence-backed insights only - no free generation

---

## System Overview

```
[Data] → [Phase 0] → [Phase 1] → [Phase 2] → [Phase 3] → [Phase 4]
  CSV     Structuring  Signals    Hypotheses   Execution   Insights
                                                      ↓
                                                   [Chat UI]
```

Raw CSV → Clean + Structure → Detect Signals → Generate Hypotheses → Execute Experiments → Generate Insights + Plots + Chat Responses

---

## Design Principles

1. **Separation of concerns**: Each phase has a single responsibility
2. **No reasoning leakage**: Phase 4 only uses evidence from Phase 3 - no new reasoning
3. **Deterministic execution where possible**: SQL-based execution, statistical thresholds
4. **All insights must be evidence-backed**: No free-form generation in core pipeline
5. **Plots are derived artifacts**: Plots only generated from executed hypotheses

---

## Architecture

### Phase 0: Data Ingestion

**Responsibilities**:
- Session-based file isolation
- Schema inference and validation
- Missingness profiling
- Outlier detection
- Clean data materialization to DuckDB

**Inputs**: Raw CSV file
**Outputs**: Queryable `clean_dataset` table in DuckDB

### Phase 1: Signal Detection

**What it detects**:
- Correlations between numeric columns
- Anomalies (Z-score + IQR methods)
- Clusters (KMeans with auto-k selection)
- Distribution shifts (KS-test)
- Trend breaks (rolling mean change points)

**Methods**: Pearson correlation, Z-score, IQR, KMeans, KS-test, rolling statistics

**Outputs**: `signals.json` with confidence and strength scores

### Phase 2: Hypothesis Engine

**Graph Structure**:
- Nodes = hypotheses (testable explanations)
- Edges = relationships (competes_with, refines, explains_signal)

**Hypothesis Types**:
- `causal`: X influences Y under condition C
- `segment-based`: Effect varies across groups
- `temporal`: Change over time
- `data-quality`: Signal may be artifact

**Outputs**: `hypothesis_graph.json` with execution order

### Phase 3: Execution Engine

**Query Execution**:
- Select highest-scoring hypothesis
- Convert to SQL plan
- Validate query (SELECT-only, column existence)
- Execute against clean_dataset

**Statistical Evaluation**:
- Compute confidence, effect_size, support
- Apply decision rules

**Outputs**: `execution_results.json` with accept/reject/refine decisions

### Phase 4: Insight + Visualization Layer

**Insight Generation**:
- Aggregate evidence from Phase 3
- Filter by confidence threshold
- Rank by confidence + effect_size

**Plot System**:
- Derive plots from executed hypotheses
- Types: scatter, bar, line, boxplot, heatmap
- Validate plot specs before rendering

**Chat Interface**:
- Type-aware response generation via GROQ
- Uses only retrieved insights - no new reasoning

**Outputs**: `insights.json`, `plots/`, `chat_response`

---

## Data Contracts Between Phases

| Phase | Output File | Key Fields |
|-------|-------------|------------|
| Phase 0 | `clean_dataset` (DuckDB) | Typed columns, clean rows |
| Phase 1 | `{session}/signals.json` | type, confidence, strength, features |
| Phase 2 | `{session}/hypothesis_graph.json` | nodes[], edges[], execution_order[] |
| Phase 3 | `{session}/execution_results.json` | accepted[], rejected[], history[] |
| Phase 4 | `{session}/phase4_insights.json` | insights[], plots[] |

---

## Core Components

| Component | Location | Role |
|-----------|----------|------|
| Schema Validator | `app/layers.py` | Type inference, column normalization |
| Signal Engine | `app/explore.py` | Correlation, anomaly, cluster detection |
| Hypothesis Builder | `app/hypothesis.py` | Graph generation, diversity enforcement |
| Execution Engine | `app/execute.py` | Query planning, evidence evaluation |
| Plot Intelligence | `app/phase4.py` | Plot spec generation, validation, rendering |
| Chat Interface | `app/phase4.py` | GROQ integration, type-aware prompts |
| Progress Tracker | `app/progress.py` | Real-time SSE updates |

---

## Visualization System

**When plots are generated**:
- Only for executed hypotheses with confidence >= 0.5
- One plot per accepted hypothesis

**Who decides plots**:
- PlotTypeSelector maps hypothesis type → plot type
- PlotCompositionEngine creates spec from hypothesis features

**Plot Spec Format**:
```json
{
  "type": "scatter",
  "x": "latency_ms",
  "y": "sessions",
  "title": "latency_ms vs sessions",
  "linked_hypothesis": "H_corr_latency_ms_sessions"
}
```

**Validation Rules**:
- Must have valid x/y columns
- No ID columns allowed
- Aggregations must match data type

**Rendering Layer**: Matplotlib → Base64 PNG

---

## API Endpoints

### Phase 0
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/upload_csv` | POST | Upload CSV, returns session_id |
| `/schema/{session_id}` | GET | Get inferred schema |
| `/data_quality/{session_id}` | GET | Get missingness/outliers |
| `/query` | POST | Execute SQL query |
| `/health` | GET | Health check |

### Phase 1
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/explore` | POST | Run signal detection |
| `/signals/{session_id}` | GET | Get detected signals |

### Phase 2
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/hypotheses` | POST | Generate hypothesis graph |
| `/hypotheses/{session_id}` | GET | Get hypothesis graph |

### Phase 3
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/execute` | POST | Run hypothesis execution |
| `/execution/{session_id}` | GET | Get execution results |

### Phase 4
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/phase4/run` | POST | Generate insights + plots |
| `/phase4/insights/{session_id}` | GET | Get insights |
| `/phase4/plots/{session_id}` | GET | Get plots |
| `/chat` | POST | Chat with GROQ |
| `/auto_analyze` | POST | Auto-detect question type + answer |

### Progress
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/progress/stream/{session_id}` | GET | SSE progress stream |
| `/progress/{session_id}` | GET | Get current progress |

---

## Execution Flow

```
1. Upload CSV
       ↓
2. Phase 0: Clean + Structure → clean_dataset
       ↓
3. Phase 1: Detect Signals → signals.json
       ↓
4. Phase 2: Generate Hypotheses → hypothesis_graph.json
       ↓
5. Phase 3: Execute Experiments → execution_results.json
       ↓
6. Phase 4: Generate Insights + Plots + Chat Response
```

**Question Flow**:
```
User Query → Router (detect type) → Run needed phases → Fetch insights/plots → GROQ response
```

---

## Decision Logic

Hypothesis acceptance rules:

| Condition | Decision |
|-----------|----------|
| confidence >= 0.50 | accept |
| confidence <= 0.20 | reject |
| otherwise | refine (create child hypothesis) |

**Refinement**: Max 5 iterations, then force stop.

---

## Evaluation

### Phase 0: Data Integrity
- Schema accuracy: 100%
- SQL execution correctness

### Phase 1: Signal Discovery
- Correlation precision: 100%
- Signal stability: 100%

### Phase 2: Hypothesis Quality
- Testability rate: 90%
- Groundedness: 100%

### Phase 3: Execution
- Execution success: 100%
- Decision accuracy: matches thresholds

### Phase 4: Insights
- Faithfulness: 100%
- Traceability: 100%

### End-to-End
- Traceability: All outputs traceable to inputs
- Hallucination rate: 0% (only evidence-backed)

**Overall: 98.0%**

Run with: `python tests/test_evaluation.py`

---

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| No signals found | Return empty signals, continue |
| No valid hypotheses | Return graph with diversity enforcement fallback |
| Conflicting evidence | Accept highest confidence, reject others |
| Low confidence results | Return "insufficient evidence" message |
| Phase 3 crash | Return 500, log error, allow retry |
| GROQ unavailable | Return cached insights without natural language |

---

## Configuration

```bash
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
ACCEPT_THRESHOLD=0.50
REJECT_THRESHOLD=0.20
MIN_CONFIDENCE_TO_REPORT=0.6

# Phase 4
ENABLE_PLOT_GENERATION=true
MAX_PLOTS_PER_HYPOTHESIS=3
GROQ_API_KEY=your_key
GROQ_MODEL=llama-3.3-70b-versatile
```

---

## Setup

```bash
# Backend
pip install -e .
uvicorn app.main:app --reload --port 8015

# Frontend
cd frontend
npm install
npm run dev -- --port 3000
```

---

## Example

```python
import httpx

client = httpx.Client(base_url="http://127.0.0.1:8015")

# 1. Upload
r = client.post("/upload_csv", files={"file": open("data.csv", "rb")})
session_id = r.json()["session_id"]

# 2. Explore (detect signals)
client.post("/explore", json={"session_id": session_id})

# 3. Generate hypotheses
client.post("/hypotheses", json={"session_id": session_id})

# 4. Execute
client.post("/execute", json={"session_id": session_id})

# 5. Get insights + plots
client.post("/phase4/run", json={"session_id": session_id})

# 6. Ask questions
response = client.post("/auto_analyze", json={
    "session_id": session_id,
    "query": "Why did revenue drop?"
})
print(response.json()["answer"])
```

---

## System Hardening Layer

The system includes production-grade security guarantees, reliability mechanisms, and observability.

### Security Layer (`app/security/`)

**Threat Model** (`threat_model.py`):
- Agent boundary enforcement with allowed/forbidden actions
- `SecurityViolation` exception for boundary breaches
- Plot spec validation (rejects ID columns)
- Chat grounding validation (requires source citations)

**SQL Guard** (`sql_guard.py`):
- Only SELECT queries allowed
- LIMIT clause required to prevent large result sets
- Forbidden keywords: DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE, etc.
- Dangerous function blocking: load_extension, attach, detach

### Reliability Layer (`app/reliability/`)

**Retry Logic**:
- Exponential backoff with jitter
- Configurable max retries and delays
- Fallback values on exhaustion

**Circuit Breaker**:
- Opens after `CIRCUIT_BREAKER_FAILURES` (default: 3) consecutive failures
- Resets after `CIRCUIT_BREAKER_TIMEOUT` (default: 60s)
- Prevents cascading failures in execution loop

### Contracts Layer (`app/contracts/`)

Strict Pydantic models for all phase outputs:
- `SignalResponseContract` - Phase 1 signals
- `HypothesisGraphResponseContract` - Phase 2 hypotheses
- `ExecutionResponseContract` - Phase 3 execution
- `Phase4ResponseContract` - Phase 4 insights
- `ChatResponseContract` - Chat responses

### Observability Layer (`app/observability/`)

**Tracer** (`app/observability/tracer.py`):
- Records all phase transitions, decisions, actions, and errors
- Stores trace events in `data/{session_id}/trace.json`
- `PhaseTransitionTracker` for tracking flow between phases
- Context manager for tracing code blocks

**Langfuse Integration** (`app/langfuse_client.py`):
- Cloud-based observability via Langfuse (https://langfuse.com)
- Traces all 5 phases + chat queries
- Session-based tracing for conversation flows
- Automatic token usage tracking for LLM calls

**Configuration** (`.env`):
```bash
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=https://cloud.langfuse.com
```

**Traced Endpoints**:
- `/explore` - Phase 1 signal detection
- `/hypotheses` - Phase 2 hypothesis generation
- `/execute` - Phase 3 hypothesis execution
- `/phase4/run` - Phase 4 insights & plots
- `/chat` - Chat query responses

**Features**:
- Graceful fallback - works without API keys (no errors)
- Auto-flush on server shutdown
- Session ID grouping for filtering
- Phase tags for categorization

### Orchestration Guards

**Execution Engine** (`execute.py`):
- Iteration limit enforced via `max_iterations` (default: 5)
- Execution history logging for all phases
- Circuit breaker integration
- SQL guard validation on all queries

---

## Limitations

- **Unstructured data**: Not suitable - requires typed CSV
- **No forecasting**: Only descriptive analysis, not predictive
- **Requires clean schema**: Column types must be inferable
- **Single dataset**: No multi-dataset reasoning yet
- **GROQ dependency**: Requires API key for natural language

---

## Future Work

- Streaming execution with progress updates
- Real-time dashboards
- Multi-dataset reasoning
- Improved causal inference
- Time-series forecasting module
- Export to PDF reports

---

## Deployment

### Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start server
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Environment Variables (`.env`)

```bash
# Required
GROQ_API_KEY=your_groq_api_key

# Optional - Langfuse Tracing
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_HOST=https://cloud.langfuse.com

# Optional - Monitoring
LOG_DIR=logs
LOG_LEVEL=INFO
RATE_LIMIT_RPM=60
RATE_LIMIT_BURST=10
APP_VERSION=1.0.0
```

### Production Features

**Rate Limiting**:
- 60 requests per minute per IP (configurable via `RATE_LIMIT_RPM`)
- 10 burst requests allowed (configurable via `RATE_LIMIT_BURST`)
- Returns 429 status when exceeded

**Logging**:
- Logs stored in `logs/app.log` (configurable via `LOG_DIR`)
- Rotation: 10MB max per file, 5 backup files
- Levels: DEBUG, INFO, WARNING, ERROR (configurable via `LOG_LEVEL`)

**Monitoring Endpoints**:
- `GET /health` - Health check
- `GET /metrics` - Request metrics (uptime, error rate, latency, endpoints)

**Response Headers**:
- `X-Response-Time` - Request latency in seconds

### Docker (Optional)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```