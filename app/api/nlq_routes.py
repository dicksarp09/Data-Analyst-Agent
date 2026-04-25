from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from app.nlq import NLQParser, NLQValidator, NLQRouter
from app.core.tracing import trace


router = APIRouter(prefix="/nlq", tags=["nlq"])

_data_dir = "data"


def get_schema_store():
    from app.main import _session_schemas
    return _session_schemas


def get_duckdb():
    from app.main import duckdb_connector
    return duckdb_connector


# Simple singleton instances
_parser = None
_validator = None
_router = None


def get_parser():
    global _parser
    if _parser is None:
        _parser = NLQParser()
    return _parser


def get_validator():
    global _validator
    if _validator is None:
        _validator = NLQValidator(get_schema_store())
    return _validator


def get_router():
    global _router
    if _router is None:
        _router = NLQRouter(_data_dir, get_duckdb(), get_schema_store())
    return _router


@router.post("/ask")
async def ask(request: dict):
    question = request.get("question", "")
    session_id = request.get("session_id", "")

    if not question:
        return {
            "answer": "question is required",
            "confidence": 0.0,
            "sources": [],
            "plots": [],
            "intent_type": "unknown"
        }
        
    if not session_id:
        return {
            "answer": "session_id is required",
            "confidence": 0.0,
            "sources": [],
            "plots": [],
            "intent_type": "unknown"
        }

    trace("nlq_ask_start", {
        "question": question,
        "session": session_id
    })

    try:
        parser = get_parser()
        nlq_router = get_router()

        intent = parser.parse(question)
        
        try:
            response = nlq_router.route(intent, session_id)
        except Exception as route_error:
            return {
                "answer": f"Router error: {str(route_error)}",
                "confidence": 0.0,
                "sources": [],
                "plots": [],
                "intent_type": intent.intent_type
            }

        trace("nlq_ask_complete", {
            "confidence": response.confidence,
            "intent": response.intent_type
        })

        return {
            "answer": response.answer,
            "confidence": response.confidence,
            "sources": response.sources,
            "plots": response.plots,
            "intent_type": response.intent_type
        }

    except Exception as e:
        trace("nlq_ask_error", {"error": str(e)})
        return {
            "answer": f"Error: {str(e)}",
            "confidence": 0.0,
            "sources": [],
            "plots": [],
            "intent_type": "error"
        }