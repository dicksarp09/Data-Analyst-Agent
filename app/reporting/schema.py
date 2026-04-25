from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime


class ReportRequest(BaseModel):
    session_id: str = Field(..., description="Session ID for the report")
    format: Literal["json", "html"] = Field("json", description="Export format")
    include_predictions: bool = Field(False, description="Include predictive analysis")
    title: Optional[str] = Field(None, description="Custom report title")


class Report(BaseModel):
    title: str = Field(..., description="Report title")
    generated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    summary: str = Field(..., description="Executive summary")
    insights: List[dict] = Field(default_factory=list, description="Validated insights")
    plots: List[dict] = Field(default_factory=list, description="Generated plots")
    predictions: List[dict] = Field(default_factory=list, description="Predictive analysis")
    stats: dict = Field(default_factory=dict, description="Dataset statistics")
    format: str = Field("json", description="Report format")


class ReportSection(BaseModel):
    title: str
    content: str
    data: Optional[dict] = None


class ReportMetadata(BaseModel):
    session_id: str
    generated_at: str
    version: str = "1.0.0"
    total_insights: int = 0
    total_plots: int = 0
    total_predictions: int = 0
