"""IEM-Only API used to feed in Meteobridge Data.

Unuseful for you all :)
"""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pyiem.database import get_dbconnc
from pyiem.observation import Observation
from pyiem.util import get_properties, utc

PROPS = {}
router = APIRouter()


def handler(key: str, data: dict):
    """Handle the request, return dict"""
    if not PROPS:
        PROPS.update(get_properties())
    lookup = {}
    for sid in ["OT0013", "OT0014", "OT0015", "OT0016"]:
        lookup[PROPS.get(f"meteobridge.key.{sid}")] = sid
    if key not in lookup:
        raise HTTPException(status_code=404, detail="BAD_KEY")
    sid = lookup[key]
    if len(data["time"]) == 14:
        _t = data["time"]
        now = utc(
            int(_t[:4]),
            int(_t[4:6]),
            int(_t[6:8]),
            int(_t[8:10]),
            int(_t[10:12]),
            int(_t[12:14]),
        )
    else:
        now = utc()
    ob = Observation(sid, "OT", now)
    for fname in [
        "tmpf",
        "max_tmpf",
        "min_tmpf",
        "dwpf",
        "relh",
        "sknt",
        "pday",
        "alti",
        "drct",
    ]:
        if data[fname] != "M":
            ob.data[fname] = float(data[fname])
    pgconn, cursor = get_dbconnc("iem", rw=True)
    ob.save(cursor)
    cursor.close()
    pgconn.commit()
    return "OK"


@router.get(
    "/meteobridge.json",
    description=__doc__,
    tags=[
        "debug",
    ],
)
def meteobridge_service(
    key: Annotated[str, Query(description="IEM Provided API Key")],
    time: Annotated[
        str, Query(description="Timestamp in YYYYMMDDHHMMSS format")
    ],
    tmpf: Annotated[str, Query(description="Temperature in Fahrenheit")],
    max_tmpf: Annotated[
        str, Query(description="Maximum Temperature in Fahrenheit")
    ],
    min_tmpf: Annotated[
        str, Query(description="Minimum Temperature in Fahrenheit")
    ],
    dwpf: Annotated[str, Query(description="Dew Point in Fahrenheit")],
    relh: Annotated[str, Query(description="Relative Humidity in Percent")],
    sknt: Annotated[str, Query(description="Wind Speed in Knots")],
    pday: Annotated[str, Query(description="Precipitation in Inches")],
    alti: Annotated[str, Query(description="Pressure in Inches of Mercury")],
    drct: Annotated[str, Query(description="Wind Direction in Degrees")],
):
    """Replaced above with __doc__."""
    return handler(
        key,
        {
            "time": time,
            "tmpf": tmpf,
            "max_tmpf": max_tmpf,
            "min_tmpf": min_tmpf,
            "dwpf": dwpf,
            "relh": relh,
            "sknt": sknt,
            "pday": pday,
            "alti": alti,
            "drct": drct,
        },
    )


meteobridge_service.__doc__ = __doc__
