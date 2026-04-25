import os
import json
from pathlib import Path
from typing import Optional
from app.reporting.schema import Report
from app.reporting.builder import ReportBuilder
from app.reporting.formatter import ReportFormatter
from app.core.tracing import trace


class ReportExporter:
    def __init__(self, data_dir: str, schema_store):
        self.data_dir = Path(data_dir)
        self.builder = ReportBuilder(data_dir, schema_store)
        self.formatter = ReportFormatter()

    def export(
        self,
        session_id: str,
        format: str = "json",
        title: Optional[str] = None
    ) -> dict:
        trace("export_start", {"session": session_id, "format": format})

        report = self.builder.build(session_id, title)

        if format == "html":
            content = self.formatter.format_html(report)
            return {
                "success": True,
                "format": "html",
                "content": content,
                "content_type": "text/html"
            }
        else:
            content = self.formatter.format_json(report)
            return {
                "success": True,
                "format": "json",
                "content": content,
                "content_type": "application/json"
            }

    def save(
        self,
        session_id: str,
        format: str = "html",
        output_dir: Optional[str] = None
    ) -> dict:
        trace("export_save_start", {"session": session_id, "format": format})

        export_result = self.export(session_id, format)
        if not export_result.get("success"):
            return export_result

        output_path = Path(output_dir) if output_dir else self.data_dir / session_id
        output_path.mkdir(parents=True, exist_ok=True)

        filename = f"report.{format}"
        file_path = output_path / filename

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(export_result["content"])

            trace("export_save_complete", {"path": str(file_path)})

            return {
                "success": True,
                "path": str(file_path),
                "format": format
            }
        except Exception as e:
            trace("export_save_error", {"error": str(e)})
            return {
                "success": False,
                "error": str(e)
            }