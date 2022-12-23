"""IEM Reanalysis hourly values by point."""
import os
import datetime

try:
    from zoneinfo import ZoneInfo  # type: ignore
except ImportError:
    from backports.zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from fastapi import Query, APIRouter, HTTPException
from pyiem import iemre
from pyiem.util import ncopen, convert_value, mm2inch
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


def get_timerange(date):
    """Figure out what period to get data for."""
    # Construct a CDT/CST Midnight to 11 PM period
    ts = datetime.datetime(
        date.year, date.month, date.day, 0, tzinfo=ZoneInfo("America/Chicago")
    )
    return ts, ts.replace(hour=23)


def workflow(sts, ets, i, j):
    """Return a dict of our data."""
    res = []

    # BUG here for Dec 31.
    fn = iemre.get_hourly_ncname(sts.year)

    if not os.path.isfile(fn):
        return res

    if i is None or j is None:
        return {"error": "Coordinates outside of domain"}

    UTC = ZoneInfo("UTC")
    with ncopen(fn) as nc:
        now = sts
        while now <= ets:
            offset = iemre.hourly_offset(now)
            res.append(
                {
                    "valid_utc": now.astimezone(UTC).strftime(ISO),
                    "valid_local": now.strftime(ISO[:-1]),
                    "skyc_%": myrounder(nc.variables["skyc"][offset, j, i], 1),
                    "air_temp_f": myrounder(
                        convert_value(
                            nc.variables["tmpk"][offset, j, i], "degK", "degF"
                        ),
                        1,
                    ),
                    "dew_point_f": myrounder(
                        convert_value(
                            nc.variables["dwpk"][offset, j, i], "degK", "degF"
                        ),
                        1,
                    ),
                    "uwnd_mps": myrounder(
                        nc.variables["uwnd"][offset, j, i], 2
                    ),
                    "vwnd_mps": myrounder(
                        nc.variables["vwnd"][offset, j, i], 2
                    ),
                    "hourly_precip_in": myrounder(
                        mm2inch(nc.variables["p01m"][offset, j, i]), 2
                    ),
                }
            )
            now += datetime.timedelta(hours=1)
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
    date: datetime.date = Query(
        ...,
        description="The CST/CDT date of interest.",
    ),
    lon: float = Query(..., description="Longitude of point of interest"),
    lat: float = Query(..., description="Latitude of point of interest"),
):
    """Do Something Fun!"""
    sts, ets = get_timerange(date)

    i, j = iemre.find_ij(lon, lat)
    if i is None or j is None:
        raise HTTPException(500, "Request outside IEMRE domain bounds.")
    df = workflow(sts, ets, i, j)
    return deliver_df(df, fmt)
