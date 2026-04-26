from pydantic import BaseModel
from typing import Optional


class UploadResponse(BaseModel):
    session_id: str
    checksum: str
    file_size: int
    message: str
    first_screen: dict = {}


class SchemaInfo(BaseModel):
    columns: list[str]
    inferred_types: dict[str, str]


class DataQualityReport(BaseModel):
    missingness: dict[str, float]
    outliers: dict[str, str]
    total_rows: int
    valid_rows: int
    quarantined_rows: int


class QueryRequest(BaseModel):
    session_id: str
    sql: str


class QueryResponse(BaseModel):
    data: list[dict]
    columns: list[str]
    row_count: int


class ExploreRequest(BaseModel):
    session_id: str


class SignalResponse(BaseModel):
    session_id: str
    signal_count: int
    signals: list[dict]
    data_summary: dict = {}
    signal_insights: list[str] = []


class HypothesisRequest(BaseModel):
    session_id: str


class HypothesisNode(BaseModel):
    id: str
    type: str
    description: str
    source_signals: list[str]
    features: list[str]
    segments: list[str]
    expected_observations: list[str]
    falsification_tests: list[str]
    status: str = "pending"
    score: float = 0.0


class HypothesisEdge(BaseModel):
    from_node: str
    to_node: str
    edge_type: str


class HypothesisGraphResponse(BaseModel):
    session_id: str
    node_count: int
    edge_count: int
    execution_order: list[str]
    nodes: list[dict]
    edges: list[dict]


class ExecuteRequest(BaseModel):
    session_id: str


class ExecutionResponse(BaseModel):
    session_id: str
    accepted_hypotheses: list
    rejected_hypotheses: list
    execution_history: list
    insights: list
    iteration: int
    status: str
    approval_required: bool = False


class Phase4Request(BaseModel):
    session_id: str


class Phase4Response(BaseModel):
    session_id: str
    insight_count: int
    plot_count: int
    insights: list
    plots: list


class ChatRequest(BaseModel):
    session_id: str
    query: str


class ChatResponse(BaseModel):
    answer: str
    confidence: float
    insights: list
    plots: list