"""List of Computed Sounding Parameters for a single location.

This service provides IEM computed sounding parameters for a given station
and optional year.  If you do not specify a year, the service will only
work for CSV output as the JSON is too large!

The following parameters are computed via MetPy and can be used when calling
the `sortby` parameter.  Please use care with these values as they are often
subject to garbage-in, garbage-out issues.

Parameter | Description
----------|------------
sbcape_jkg | Surface Based CAPE (J/kg)
sbcin_jkg | Surface Based CIN (J/kg)
mucape_jkg | Most Unstable CAPE (J/kg)
mucin_jkg | Most Unstable CIN (J/kg)
pwater_mm | Precipitable Water (mm)
lcl_agl_m | LCL Height (m AGL)
lfc_agl_m | LFC Height (m AGL)
el_agl_m | EL Height (m AGL)
total_totals | Total Totals Index
sweat_index | Sweat Index
bunkers_lm_smps | Bunkers Left Mover Storm Motion (m/s)
bunkers_rm_smps | Bunkers Right Mover Storm Motion (m/s)
mean_sfc_6km_smps | Mean Wind Speed from the Surface to 6km (m/s)
srh_sfc_1km_total | Surface to 1km Storm Relative Helicity (m^2/s^2)
srh_sfc_3km_total | Surface to 3km Storm Relative Helicity (m^2/s^2)
shear_sfc_1km_smps | Surface to 1km Shear (m/s)
shear_sfc_3km_smps | Surface to 3km Shear (m/s)
shear_sfc_6km_smps | Surface to 6km Shear (m/s)

"""

from datetime import date
from enum import Enum
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem.database import sql_helper
from pyiem.network import Table as NetworkTable
from pyiem.util import utc

from ..models import SupportedFormatsNoGeoJSON
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


class SortByParameter(str, Enum):
    sbcape_jkg = "sbcape_jkg"
    sbcin_jkg = "sbcin_jkg"
    mucape_jkg = "mucape_jkg"
    mucin_jkg = "mucin_jkg"
    pwater_mm = "pwater_mm"
    lcl_agl_m = "lcl_agl_m"
    lfc_agl_m = "lfc_agl_m"
    el_agl_m = "el_agl_m"
    total_totals = "total_totals"
    sweat_index = "sweat_index"
    bunkers_lm_smps = "bunkers_lm_smps"
    bunkers_rm_smps = "bunkers_rm_smps"
    mean_sfc_6km_smps = "mean_sfc_6km_smps"
    srh_sfc_1km_total = "srh_sfc_1km_total"
    srh_sfc_3km_total = "srh_sfc_3km_total"
    shear_sfc_1km_smps = "shear_sfc_1km_smps"
    shear_sfc_3km_smps = "shear_sfc_3km_smps"
    shear_sfc_6km_smps = "shear_sfc_6km_smps"


def handler(
    station: str,
    year: Optional[int],
    sortby: Optional[SortByParameter],
    limit: int,
    asc: bool,
) -> pd.DataFrame:
    """Handle the request, return dict"""
    params = {
        "station": station,
        "sts": None,
        "ets": None,
        "sortby": sortby,
        "limit": 999_999 if sortby is None else limit,
    }
    station_limiter = "station = :station"
    if station.startswith("_"):
        station_limiter = "station = ANY(:station)"
        nt = NetworkTable("RAOB", only_online=False)
        params["station"] = (
            nt.sts[station]["name"].split("--")[1].strip().split()
        )

    time_limiter = ""
    order_by = "valid ASC"
    if sortby is not None:
        order_by = f"{sortby.value} {'ASC' if asc else 'DESC'} NULLS LAST"
    elif year is not None:
        time_limiter = " and valid >= :sts and valid < :ets"
        params["sts"] = utc(year, 1, 1)
        params["ets"] = utc(year + 1, 1, 1)
    with get_sqlalchemy_conn("raob") as pgconn:
        df = pd.read_sql(
            sql_helper(
                """
    SELECT * from raob_flights where {station_limiter}
    {time_limiter} ORDER by {order_by} LIMIT :limit
    """,
                station_limiter=station_limiter,
                time_limiter=time_limiter,
                order_by=order_by,
            ),
            pgconn,
            params=params,
        )

    return df


@router.get(
    "/raobs_by_year.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def nwstext_service(
    fmt: SupportedFormatsNoGeoJSON,
    station: str = Query(..., max_length=4, min_length=4),
    year: Optional[int] = Query(None, ge=1947, le=date.today().year),
    sortby: Optional[SortByParameter] = Query(
        None,
        description="Sort by Parameter, year is ignored",
    ),
    limit: Optional[int] = Query(
        100,
        description="Limit to the number of results returned, only for sortby",
        le=1000,
        ge=1,
    ),
    asc: Optional[bool] = Query(
        False,
        description="Sort in ascending order, only for sortby",
    ),
):
    """Replaced above by __doc__."""
    if fmt == "json" and year is None and sortby is None:
        raise HTTPException(
            status_code=422, detail="JSON requires a year or sortby set"
        )
    return deliver_df(handler(station, year, sortby, limit, asc), fmt)


nwstext_service.__doc__ = __doc__
