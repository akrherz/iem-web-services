"""Helpers."""

from pyiem import util
from fastapi import HTTPException


def get_dbconn(name):
    """Get a database connection or fail."""
    try:
        return util.get_dbconn(name)
    except Exception:
        raise HTTPException(status_code=503, detail="Database unavailable.")
