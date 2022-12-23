"""IEM Reanalysis single Day values by point."""
import os
import datetime

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query, HTTPException
from pyiem import iemre
from pyiem.util import ncopen, convert_value, mm2inch
import pyiem.prism as prismutil
from ...models import SupportedFormatsNoGeoJSON
from ...util import deliver_df

router = APIRouter()


def myrounder(val, precision):
    """round a float or give back None"""
    if val is None or np.isnan(val) or np.ma.is_masked(val):
        return None
    return round(val, precision)


@router.get(
    "/iemre/daily.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    date: datetime.date = Query(..., description="The date of interest."),
    lon: float = Query(..., description="Longitude of point of interest"),
    lat: float = Query(..., description="Latitude of point of interest"),
):
    """Do Something Fun!"""

    i, j = iemre.find_ij(lon, lat)
    if i is None or j is None:
        raise HTTPException(500, "Request outside IEMRE domain bounds.")
    offset = iemre.daily_offset(date)

    res = []

    fn = iemre.get_daily_ncname(date.year)

    if date.year > 1980:
        ncfn = f"/mesonet/data/prism/{date.year}_daily.nc"
        if not os.path.isfile(ncfn):
            prism_precip = None
        else:
            i2, j2 = prismutil.find_ij(lon, lat)
            with ncopen(ncfn) as nc:
                prism_precip = mm2inch(nc.variables["ppt"][offset, j2, i2])
    else:
        prism_precip = None

    if date.year > 2000:
        ncfn = iemre.get_daily_mrms_ncname(date.year)
        if not os.path.isfile(ncfn):
            mrms_precip = None
        else:
            j2 = int((lat - iemre.SOUTH) * 100.0)
            i2 = int((lon - iemre.WEST) * 100.0)
            with ncopen(ncfn) as nc:
                mrms_precip = mm2inch(nc.variables["p01d"][offset, j2, i2])
    else:
        mrms_precip = None

    c2000 = date.replace(year=2000)
    coffset = iemre.daily_offset(c2000)

    with ncopen(fn) as nc:
        with ncopen(iemre.get_dailyc_ncname()) as cnc:

            res.append(
                {
                    "prism_precip_in": myrounder(prism_precip, 2),
                    "mrms_precip_in": myrounder(mrms_precip, 2),
                    "daily_high_f": myrounder(
                        convert_value(
                            nc.variables["high_tmpk"][offset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "12z_high_f": myrounder(
                        convert_value(
                            nc.variables["high_tmpk_12z"][offset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "climate_daily_high_f": myrounder(
                        convert_value(
                            cnc.variables["high_tmpk"][coffset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "daily_low_f": myrounder(
                        convert_value(
                            nc.variables["low_tmpk"][offset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "12z_low_f": myrounder(
                        convert_value(
                            nc.variables["low_tmpk_12z"][offset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "avg_dewpoint_f": myrounder(
                        convert_value(
                            nc.variables["avg_dwpk"][offset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "climate_daily_low_f": myrounder(
                        convert_value(
                            cnc.variables["low_tmpk"][coffset, j, i],
                            "degK",
                            "degF",
                        ),
                        1,
                    ),
                    "daily_precip_in": myrounder(
                        mm2inch(nc.variables["p01d"][offset, j, i]), 2
                    ),
                    "12z_precip_in": myrounder(
                        mm2inch(nc.variables["p01d_12z"][offset, j, i]), 2
                    ),
                    "climate_daily_precip_in": myrounder(
                        mm2inch(cnc.variables["p01d"][coffset, j, i]), 2
                    ),
                    "srad_mj": myrounder(
                        nc.variables["rsds"][offset, j, i]
                        * 86400.0
                        / 1000000.0,
                        2,
                    ),
                    "avg_windspeed_mps": myrounder(
                        nc.variables["wind_speed"][offset, j, i], 2
                    ),
                }
            )
    return deliver_df(pd.DataFrame(res), fmt)
