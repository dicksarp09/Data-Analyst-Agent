from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class NLQIntent(BaseModel):
    intent_type: Literal[
        "explain",
        "compare",
        "trend",
        "anomaly",
        "metric_lookup",
        "predict",
        "simulation"
    ] = Field(..., description="Type of user intent")
    target_metric: Optional[str] = Field(None, description="Primary metric being asked about")
    dimensions: Optional[list] = Field(None, description="Dimensions for breakdown")
    time_range: Optional[str] = Field(None, description="Time range specification")
    requires_full_pipeline: bool = Field(False, description="Whether full analysis pipeline is needed")
    raw_question: str = Field(..., description="Original user question")


class NLQResponse(BaseModel):
    answer: str = Field(..., description="Grounded answer from Phase 4")
    confidence: float = Field(..., description="Confidence score 0-1")
    sources: List[str] = Field(default_factory=list, description="Insight IDs used")
    plots: List[str] = Field(default_factory=list, description="Plot IDs used")
    intent_type: str = Field(..., description="Detected intent type")
    traced: bool = Field(True, description="Whether this response was traced")
