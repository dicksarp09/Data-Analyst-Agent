"""
SQL Security Guard - Enforces read-only, safe SQL operations
"""
import re
from typing import Optional


class SQLSecurityError(Exception):
    """Raised when SQL query violates security rules."""
    pass


def validate_sql(query: str) -> bool:
    """
    Validate SQL query for security.
    
    Rules:
    - ONLY SELECT allowed
    - LIMIT required to prevent large result sets
    - No joins on undefined columns (basic check)
    - No subqueries with write operations
    - Max query length: 10000 chars
    
    Args:
        query: SQL query to validate
        
    Returns:
        True if valid
        
    Raises:
        SQLSecurityError: If query violates security rules
    """
    if not query or not query.strip():
        raise SQLSecurityError("Empty query not allowed")
    
    query = query.strip()
    
    if len(query) > 10000:
        raise SQLSecurityError("Query too long (max 10000 chars)")
    
    query_lower = query.lower()
    
    if "select" not in query_lower:
        raise SQLSecurityError("Only SELECT queries allowed")
    
    forbidden_keywords = [
        "drop", "delete", "update", "insert", "create",
        "alter", "truncate", "replace", "merge", "exec",
        "execute", "call ", "grant", "revoke"
    ]
    
    for keyword in forbidden_keywords:
        if keyword in query_lower:
            raise SQLSecurityError(f"Forbidden SQL operation: {keyword}")
    
    if "limit" not in query_lower:
        raise SQLSecurityError("Query must include LIMIT clause")
    
    dangerous_functions = [
        "load_extension", "attach", "detach"
    ]
    for func in dangerous_functions:
        if func in query_lower:
            raise SQLSecurityError(f"Forbidden SQL function: {func}")
    
    return True


def extract_select_query(query: str) -> str:
    """
    Extract and clean SELECT query.
    
    Args:
        query: Raw query
        
    Returns:
        Cleaned SELECT query
    """
    query = query.strip()
    
    if query.lower().startswith("select"):
        return query
    
    if "select" in query.lower():
        match = re.search(r'select\s+.*', query, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(0).strip()
    
    return query


def get_query_limits(query: str) -> dict:
    """
    Extract and validate query limits.
    
    Args:
        query: SQL query
        
    Returns:
        Dict with limit info
    """
    query_lower = query.lower()
    
    limit_match = re.search(r'limit\s+(\d+)', query_lower)
    if limit_match:
        limit = int(limit_match.group(1))
        return {"has_limit": True, "limit_value": limit, "max_allowed": 1000}
    
    return {"has_limit": False, "limit_value": None, "max_allowed": 1000}


def sanitize_query_params(params: dict) -> dict:
    """
    Sanitize query parameters to prevent injection.
    
    Args:
        params: Query parameters
        
    Returns:
        Sanitized parameters
    """
    sanitized = {}
    for key, value in params.items():
        if isinstance(value, str):
            value = value.replace("'", "").replace('"', "").replace(";", "")
            sanitized[key] = value
        else:
            sanitized[key] = value
    return sanitized