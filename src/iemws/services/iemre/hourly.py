"""IEM Reanalysis hourly values by point."""

import os
from datetime import date as dateobj
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem import iemre
from pyiem.util import convert_value, mm2inch, ncopen

from ...models import SupportedFormatsNoGeoJSON
from ...models.iemre import HourlySchema
from ...util import deliver_df

ISO = "%Y-%m-%dT%H:%MZ"
router = APIRouter()


def myrounder(val, precision):
    """round a float or give back None"""
    if val is None or np.isnan(val) or np.ma.is_masked(val):
        return None
    return round(float(val), precision)


def get_timerange(date, domain):
    """Figure out what period to get data for."""
    ts = datetime(
        date.year,
        date.month,
        date.day,
        0,
        tzinfo=iemre.DOMAINS[domain]["tzinfo"],
    )
    return ts, ts.replace(hour=23)


def workflow(sts, ets, i, j, domain):
    """Return a dict of our data."""
    res = []

    # BUG here for Dec 31.
    fn = iemre.get_hourly_ncname(sts.year, domain=domain)

    if not os.path.isfile(fn):
        raise HTTPException(400, "No data.")

    if i is None or j is None:
        raise HTTPException(400, "Request outside IEMRE domain bounds.")

    UTC = ZoneInfo("UTC")
    times = []
    now = sts
    while now <= ets:
        times.append((now, iemre.hourly_offset(now)))
        now += timedelta(hours=1)
    offsets = [t[1] for t in times]
    lo = min(offsets)
    hi = max(offsets) + 1
    data = {}
    # One contiguous read per variable instead of one per hour
    with ncopen(fn, timeout=5) as nc:
        for vname in ["skyc", "tmpk", "dwpk", "uwnd", "vwnd", "p01m"]:
            data[vname] = nc.variables[vname][lo:hi, j, i]
    for ts, offset in times:
        k = offset - lo
        res.append(
            {
                "valid_utc": ts.astimezone(UTC).strftime(ISO),
                "valid_local": ts.strftime(ISO[:-1]),
                "skyc_%": myrounder(data["skyc"][k], 1),
                "air_temp_f": myrounder(
                    convert_value(data["tmpk"][k], "degK", "degF"), 1
                ),
                "dew_point_f": myrounder(
                    convert_value(data["dwpk"][k], "degK", "degF"), 1
                ),
                "uwnd_mps": myrounder(data["uwnd"][k], 2),
                "vwnd_mps": myrounder(data["vwnd"][k], 2),
                "hourly_precip_in": myrounder(mm2inch(data["p01m"][k]), 2),
            }
        )
    return pd.DataFrame(res)


@router.get(
    "/iemre/hourly.{fmt}",
    response_model=HourlySchema,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    date: dateobj = Query(
        ...,
        description="The CST/CDT date of interest.",
    ),
    lon: float = Query(
        ..., description="Longitude of point of interest", ge=-180, le=180
    ),
    lat: float = Query(
        ..., description="Latitude of point of interest", ge=-90, le=90
    ),
):
    """Do Something Fun!"""
    domain = iemre.get_domain(lon, lat)
    if domain is None:
        raise HTTPException(400, "Request outside IEMRE domain bounds.")
    sts, ets = get_timerange(date, domain)
    i, j = iemre.find_ij(lon, lat, domain=domain)
    df = workflow(sts, ets, i, j, domain)
    return deliver_df(df, fmt)
