from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime


class EventType(str, Enum):
    INSIGHT_GENERATED = "insight_generated"
    ANALYSIS_COMPLETE = "analysis_complete"
    PIPELINE_PROGRESS = "pipeline_progress"
    ERROR = "error"
    REPORT_READY = "report_ready"


class Event(BaseModel):
    type: EventType = Field(..., description="Event type")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    payload: Dict[str, Any] = Field(default_factory=dict)
    session_id: Optional[str] = Field(None)


class EventPayload(BaseModel):
    insight_text: Optional[str] = None
    confidence: Optional[float] = None
    hypothesis_type: Optional[str] = None
    plot_count: int = 0
    insight_count: int = 0
    progress_percent: Optional[int] = None
    error_message: Optional[str] = None
    report_path: Optional[str] = None