"""Simple ping/pong style service returning the server's time."""
from datetime import datetime

from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/servertime",
    description=__doc__,
    tags=[
        "debug",
    ],
)
def time_service():
    """Babysteps."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


time_service.__doc__ = __doc__
