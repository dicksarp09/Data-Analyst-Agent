import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from collections import defaultdict


class ProgressStore:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self._current: dict = {}
    
    def set_phase(self, session_id: str, phase: str, message: str = ""):
        self._current[session_id] = {
            "phase": phase,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "logs": [],
            "stats": {}
        }
    
    def add_log(self, session_id: str, log: str):
        if session_id not in self._current:
            self.set_phase(session_id, "unknown", "")
        self._current[session_id]["logs"].append({
            "message": log,
            "timestamp": datetime.now().isoformat()
        })
    
    def set_stats(self, session_id: str, stats: dict):
        if session_id in self._current:
            self._current[session_id]["stats"] = stats
    
    def get_progress(self, session_id: str) -> dict:
        return self._current.get(session_id, {
            "phase": "idle",
            "message": "",
            "logs": [],
            "stats": {}
        })
    
    def clear(self, session_id: str):
        if session_id in self._current:
            del self._current[session_id]


progress_store = ProgressStore()


async def progress_generator(session_id: str):
    """Generator for SSE events"""
    last_log_count = 0
    while True:
        progress = progress_store.get_progress(session_id)
        
        # Only yield if there are new logs
        current_log_count = len(progress.get("logs", []))
        if current_log_count != last_log_count:
            yield {"event": "progress", "data": json.dumps(progress)}
            last_log_count = current_log_count
        else:
            yield {"event": "ping", "data": json.dumps({"phase": progress.get("phase", "idle")})}
        
        await asyncio.sleep(0.5)