from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field
from typing import Optional
from app.reporting import ReportExporter
from app.core.tracing import trace


router = APIRouter(prefix="/report", tags=["reporting"])

_data_dir = "data"


def get_schema_store():
    from app.main import _session_schemas
    return _session_schemas


def get_exporter():
    return ReportExporter(_data_dir, get_schema_store())


@router.get("/{session_id}")
async def get_report(
    session_id: str,
    format: str = "json"
):
    trace("report_request", {"session": session_id, "format": format})

    try:
        exporter = get_exporter()
        result = exporter.export(session_id, format)

        if not result.get("success"):
            return {
                "success": False,
                "error": result.get("error", "Unknown error"),
                "format": format
            }

        return Response(
            content=result["content"],
            media_type=result["content_type"]
        )

    except Exception as e:
        trace("report_error", {"error": str(e)})
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/export")
async def export_report(request: dict):
    session_id = request.get("session_id")
    report_format = request.get("format", "json")
    title = request.get("title")

    if not session_id:
        raise HTTPException(status_code=400, detail="session_id required")

    trace("report_export", {"session": session_id, "format": report_format})

    try:
        exporter = get_exporter()
        result = exporter.export(session_id, report_format, title)

        if not result.get("success"):
            raise HTTPException(status_code=400, detail=result.get("error"))

        return result

    except HTTPException:
        raise
    except Exception as e:
        trace("report_export_error", {"error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))