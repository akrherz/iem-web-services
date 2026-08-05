"""Simple ping/pong style service returning the server's time."""

from fastapi import APIRouter
from pyiem.reference import ISO8601
from pyiem.util import utc

router = APIRouter()


@router.get(
    "/servertime",
    description=__doc__,
    tags=[
        "debug",
    ],
)
def time_service():
    """Unused docstring."""
    return utc().strftime(ISO8601)
