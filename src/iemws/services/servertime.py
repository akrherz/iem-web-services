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
    """Babysteps."""
    return utc().strftime(ISO8601)


time_service.__doc__ = __doc__
