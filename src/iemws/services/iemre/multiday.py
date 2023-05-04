"""IEM Reanalysis multi-day values by point."""
import datetime

import numpy as np
import pandas as pd
import pyiem.prism as prismutil
from fastapi import APIRouter, HTTPException, Query
from pyiem import iemre
from pyiem.util import convert_value, mm2inch, ncopen

from ...models import SupportedFormatsNoGeoJSON
from ...util import deliver_df

router = APIRouter()


def clean(val):
    """My filter"""
    if val is None or np.isnan(val) or np.ma.is_masked(val):
        return None
    return float(val)


@router.get(
    "/iemre/multiday.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    sdate: datetime.date = Query(..., description="Start Date."),
    edate: datetime.date = Query(..., description="End Date."),
    lon: float = Query(..., description="Longitude of point of interest"),
    lat: float = Query(..., description="Latitude of point of interest"),
):
    """Go Main Go"""
    # Make sure we aren't in the future
    tsend = datetime.date.today()
    if edate > tsend:
        edate = datetime.date.today() - datetime.timedelta(days=1)

    i, j = iemre.find_ij(lon, lat)
    if i is None or j is None:
        raise HTTPException(500, "Request outside IEMRE domain bounds.")
    offset1 = iemre.daily_offset(sdate)
    offset2 = iemre.daily_offset(edate) + 1
    # Get our netCDF vars
    with ncopen(iemre.get_daily_ncname(sdate.year)) as nc:
        hightemp = convert_value(
            nc.variables["high_tmpk"][offset1:offset2, j, i], "degK", "degF"
        )
        high12temp = convert_value(
            nc.variables["high_tmpk_12z"][offset1:offset2, j, i],
            "degK",
            "degF",
        )
        lowtemp = convert_value(
            nc.variables["low_tmpk"][offset1:offset2, j, i], "degK", "degF"
        )
        low12temp = convert_value(
            nc.variables["low_tmpk_12z"][offset1:offset2, j, i], "degK", "degF"
        )
        precip = mm2inch(nc.variables["p01d"][offset1:offset2, j, i])
        precip12 = mm2inch(nc.variables["p01d_12z"][offset1:offset2, j, i])

    # Get our climatology vars
    c2000 = sdate.replace(year=2000)
    coffset1 = iemre.daily_offset(c2000)
    c2000 = edate.replace(year=2000)
    coffset2 = iemre.daily_offset(c2000) + 1
    with ncopen(iemre.get_dailyc_ncname()) as cnc:
        chigh = convert_value(
            cnc.variables["high_tmpk"][coffset1:coffset2, j, i], "degK", "degF"
        )
        clow = convert_value(
            cnc.variables["low_tmpk"][coffset1:coffset2, j, i], "degK", "degF"
        )
        cprecip = mm2inch(
            cnc.variables["p01d"][coffset1:coffset2, j, i],
        )

    if sdate.year > 1980:
        i2, j2 = prismutil.find_ij(lon, lat)
        with ncopen(f"/mesonet/data/prism/{sdate.year}_daily.nc") as nc:
            prism_precip = mm2inch(
                nc.variables["ppt"][offset1:offset2, j2, i2],
            )
    else:
        prism_precip = [None] * (offset2 - offset1)

    if sdate.year > 2000:
        j2 = int((lat - iemre.SOUTH) * 100.0)
        i2 = int((lon - iemre.WEST) * 100.0)
        with ncopen(iemre.get_daily_mrms_ncname(sdate.year)) as nc:
            mrms_precip = mm2inch(
                nc.variables["p01d"][offset1:offset2, j2, i2],
            )
    else:
        mrms_precip = [None] * (offset2 - offset1)

    res = []

    for i in range(0, offset2 - offset1):
        now = sdate + datetime.timedelta(days=i)
        res.append(
            {
                "date": now.strftime("%Y-%m-%d"),
                "mrms_precip_in": clean(mrms_precip[i]),
                "prism_precip_in": clean(prism_precip[i]),
                "daily_high_f": clean(hightemp[i]),
                "12z_high_f": clean(high12temp[i]),
                "climate_daily_high_f": clean(chigh[i]),
                "daily_low_f": clean(lowtemp[i]),
                "12z_low_f": clean(low12temp[i]),
                "climate_daily_low_f": clean(clow[i]),
                "daily_precip_in": clean(precip[i]),
                "12z_precip_in": clean(precip12[i]),
                "climate_daily_precip_in": clean(cprecip[i]),
            }
        )
    return deliver_df(pd.DataFrame(res), fmt)
