from pydantic import BaseModel, Field, field_validator
from typing import Optional


class SignalContract(BaseModel):
    type: str
    pair: Optional[list] = None
    feature: Optional[str] = None
    feature_group: Optional[list] = None
    strength: float = 0.0
    p_value: Optional[float] = None
    method: str
    confidence: float = 0.0
    severity: Optional[str] = None
    clusters: Optional[int] = None
    shift_score: Optional[float] = None
    anomaly_count: Optional[int] = None
    total_count: Optional[int] = None
    timestamp: Optional[str] = None
    change_magnitude: Optional[float] = None


class SchemaInfoContract(BaseModel):
    columns: list[str]
    inferred_types: dict[str, str]
    semantic_roles: dict[str, str] = {}


class SignalResponseContract(BaseModel):
    session_id: str
    signal_count: int
    signals: list[SignalContract]
    data_summary: dict = {}
    signal_insights: list[str] = []


class HypothesisNodeContract(BaseModel):
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


class HypothesisEdgeContract(BaseModel):
    from_node: str
    to_node: str
    edge_type: str


class HypothesisGraphContract(BaseModel):
    nodes: list[HypothesisNodeContract]
    edges: list[HypothesisEdgeContract]


class HypothesisGraphResponseContract(BaseModel):
    session_id: str
    node_count: int
    edge_count: int
    execution_order: list[str]
    hypothesis_graph: HypothesisGraphContract


class ExecutionEntryContract(BaseModel):
    hypothesis_id: str
    action: str
    evidence: dict
    iteration: int
    sql: Optional[str] = None
    error: Optional[str] = None


class InsightContract(BaseModel):
    insight: str
    confidence: float
    hypothesis_id: str
    effect_size: float
    type: str
    generated_at: str


class ExecutionResponseContract(BaseModel):
    session_id: str
    accepted_hypotheses: list[str]
    rejected_hypotheses: list[str]
    execution_history: list[ExecutionEntryContract]
    insights: list[InsightContract]
    iteration: int
    status: str


class PlotContract(BaseModel):
    plot_id: str
    type: str
    title: str
    format: str
    data: str
    linked_hypothesis: str
    insight_anchor: bool = True


class Phase4ResponseContract(BaseModel):
    session_id: str
    insight_count: int
    plot_count: int
    insights: list[InsightContract]
    plots: list[PlotContract]
    contradiction: dict


class ChatResponseContract(BaseModel):
    answer: str
    confidence: float
    insights: list[str]
    plots: list[str]
    sources: list[str] = []