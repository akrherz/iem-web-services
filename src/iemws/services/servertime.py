"""Simple ping/pong style service returning the server's time."""

from fastapi import APIRouter, Query
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
def time_service(
    opt: str = Query(None, description="For testing purposes."),
):
    """Babysteps."""
    if opt == "fail":
        raise Exception("This is a test of the emergency broadcast system!")
    return utc().strftime(ISO8601)


time_service.__doc__ = __doc__
