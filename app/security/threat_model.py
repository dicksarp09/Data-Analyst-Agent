"""
Security Layer - Agent Boundary Enforcement
Defines what the system can and cannot do.
"""
from typing import List
from functools import wraps


AGENT_BOUNDARY = {
    "allowed_actions": [
        "read_duckdb",
        "run_select_queries",
        "generate_plot_specs",
        "read_schema_metadata",
        "validate_schema",
        "compute_statistics",
        "detect_signals",
        "generate_hypotheses",
        "evaluate_evidence",
        "render_plots",
        "aggregate_insights",
    ],
    "forbidden_actions": [
        "write_database",
        "delete_data",
        "external_api_calls",
        "filesystem_access_outside_session",
        "modify_duckdb",
        "drop_table",
        "create_table",
        "execute_ddl",
        "call_external_llm_directly",
        "write_filesystem",
    ]
}


class SecurityViolation(Exception):
    """Raised when an action violates the agent boundary."""
    pass


def enforce_agent_boundary(action: str) -> bool:
    """
    Enforce the agent boundary by checking if action is allowed.
    
    Args:
        action: The action to check
        
    Returns:
        True if allowed
        
    Raises:
        SecurityViolation: If action is forbidden
    """
    if action in AGENT_BOUNDARY["forbidden_actions"]:
        raise SecurityViolation(f"Forbidden action: {action}. Allowed: {AGENT_BOUNDARY['allowed_actions']}")
    return True


def with_security_check(action_name: str):
    """
    Decorator to enforce security boundary on a function.
    
    Usage:
        @with_security_check("read_duckdb")
        def execute_query(query):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            enforce_agent_boundary(action_name)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def validate_sql_action(query: str) -> bool:
    """
    Validate that a SQL query is a read-only operation.
    
    Args:
        query: SQL query to validate
        
    Returns:
        True if valid
        
    Raises:
        SecurityViolation: If query contains forbidden operations
    """
    query_lower = query.lower().strip()
    
    forbidden_keywords = [
        "drop", "delete", "update", "insert", "create", 
        "alter", "truncate", "replace", "merge"
    ]
    
    for keyword in forbidden_keywords:
        if keyword in query_lower:
            raise SecurityViolation(f"Forbidden SQL keyword in query: {keyword}")
    
    return True


def validate_plot_spec(spec: dict, schema: dict) -> bool:
    """
    Validate that a plot specification is safe.
    
    Args:
        spec: Plot specification
        schema: Data schema
        
    Returns:
        True if valid
        
    Raises:
        SecurityViolation: If spec is invalid or unsafe
    """
    x_col = spec.get("x", "")
    y_col = spec.get("y", "")
    
    id_patterns = ["id", "_id", "uuid", "key", "user_id", "session_id"]
    
    for col in [x_col, y_col]:
        if col and any(p in col.lower() for p in id_patterns):
            raise SecurityViolation(f"ID column cannot be used in plots: {col}")
    
    if schema:
        columns = schema.get("columns", [])
        for col in [x_col, y_col]:
            if col and col not in columns:
                raise SecurityViolation(f"Invalid column in plot spec: {col}")
    
    return True


def validate_chat_grounding(response: dict, insights: List[dict]) -> bool:
    """
    Validate that chat response is grounded in insights.
    
    Args:
        response: Chat response to validate
        insights: List of insights to reference
        
    Returns:
        True if grounded
        
    Raises:
        SecurityViolation: If response is ungrounded
    """
    answer = response.get("answer", "")
    
    if not insights and len(answer) > 50:
        raise SecurityViolation("Chat response without insights is not allowed")
    
    if "because" in answer.lower() and "hypothesis" not in answer.lower():
        if not response.get("sources"):
            raise SecurityViolation("Ungrounded explanation detected - no sources cited")
    
    return True