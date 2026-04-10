"""Flash Flood Guidance by point.

The IEM caches the grib file source of Flash Flood Guidance issued by the
NWS River Forecast Centers.  If you do not provide a valid timestamp, it will
assume you want the latest forecast.  If you provide a valid timestamp, the
service will look for the nearest forecast made within the past 24 hours of
the provided time.
"""

from datetime import datetime, timedelta
from typing import Annotated

import numpy as np
import pygrib
import pyproj
from fastapi import APIRouter, HTTPException, Query
from pyiem.reference import ISO8601
from pyiem.util import archive_fetch, utc

router = APIRouter()


def locate_gribfile(valid: datetime) -> tuple[datetime | None, str | None]:
    """Figure out which file we have for this valid timestamp."""
    # Rectify to six hourly
    valid = valid.replace(hour=valid.hour - valid.hour % 6)
    for hr in range(0, 25, 6):
        lvalid = valid - timedelta(hours=hr)
        ppath = lvalid.strftime("%Y/%m/%d/model/ffg/5kmffg_%Y%m%d%H.grib2")
        with archive_fetch(ppath, method="HEAD") as testfn:
            if testfn == "":
                return lvalid, ppath
    return None, None


def handler(valid: datetime, lon: float, lat: float):
    """Handle the request, return dict"""
    res = {"ffg": []}
    lvalid, ppath = locate_gribfile(valid)
    if lvalid is None:
        raise HTTPException(
            status_code=404,
            detail="unable to find grib file to use for valid time",
        )
    res["forecast_initial_time"] = lvalid.strftime(ISO8601)
    res["grib_source"] = (
        f"https://mesonet.agron.iastate.edu/archive/data/{ppath}"
    )
    idxx, idxy = None, None
    with archive_fetch(ppath) as gribfn, pygrib.open(gribfn) as grbs:
        for grb in grbs:
            if idxx is None:
                proj = pyproj.Proj(grb.projparams)
                lat1 = grb["latitudeOfFirstGridPointInDegrees"]
                lon1 = grb["longitudeOfFirstGridPointInDegrees"]
                llcrnrx, llcrnry = proj(lon1, lat1)
                dx = grb["DxInMetres"]
                dy = grb["DyInMetres"]
                x, y = proj(lon, lat)
                idxx = int((x - llcrnrx) / dx)
                idxy = int((y - llcrnry) / dy)
                res["gridx"] = idxx
                res["gridy"] = idxy

            val = grb.values[idxy, idxx]
            if np.ma.is_masked(val) or np.isnan(val):
                val = None
            hr = int(grb["stepRange"].split("-")[1])
            res["ffg"].append(
                {"stepRange": grb["stepRange"], "hour": hr, "ffg_mm": val}
            )

    return res


@router.get(
    "/ffg_bypoint.json",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def ffg_bypoint_service(
    lon: Annotated[
        float, Query(ge=-140, le=-60, description="Longitude degree E")
    ],
    lat: Annotated[
        float, Query(ge=10, le=80, description="Latitude degrees N")
    ],
    valid: Annotated[datetime | None, Query(description="Date Time")] = None,
):
    """Replaced above."""
    if valid is None:
        valid = utc()
    return handler(valid, lon, lat)


ffg_bypoint_service.__doc__ = __doc__
