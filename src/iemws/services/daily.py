"""IEM Daily Summary Service.

This API service returns IEM computed and network provided daily summary
information.  There are a number of ways to approach this app:

- `/api/1/daily.geojson?date=2021-06-02&network=IA_ASOS` - Get all IA_ASOS
stations for date in GeoJSON.
- `/api/1/daily.json?station=AMW&network=IA_ASOS&year=2021` - Get all Ames
ASOS data for 2021.
- `/api/1/daily.json?station=AMW&network=IA_ASOS&year=2021&month=6` - Get all
Ames ASOS data for June 2021.

Note that this service can emit GeoJSON, but sometimes that format does not
make much sense, for example when requesting just one station's worth of data.

"""

import re
from datetime import date as dateobj
from datetime import timedelta

import geopandas as gpd
from fastapi import APIRouter, HTTPException, Query
from pyiem.database import sql_helper

from ..models import SupportedFormats
from ..models.daily import DailySchema
from ..util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()

CLIMATE_NETWORK_RE = re.compile(r"^[A-Z]{2}CLIMATE$")


def get_df(network: str, station, dt, month, year):
    """Handle the request, return dict"""
    params = {
        "station": station,
        "network": network,
        "day": dt,
        "year": year,
        "month": month,
    }
    if CLIMATE_NETWORK_RE.match(network):
        sl = " and station = :station " if station is not None else ""
        dl = ""
        if dt is not None:
            dl = " and day = :day "
        elif month is None and year is not None:
            dl = " and year = :year "
        elif month is not None and year is not None:
            dl = " and year = :year and month = :month "
        with get_sqlalchemy_conn("coop") as conn:
            table = f"alldata_{network[:2].lower()}"
            df = gpd.read_postgis(
                sql_helper(
                    """
                SELECT station, to_char(day, 'YYYY-mm-dd') as date,
                high as max_tmpf, low as min_tmpf,
                temp_estimated as tmpf_est, precip_estimated as precip_est,
                precip, null as max_gust, snow, snowd, null as min_rh,
                null as max_rh, null as max_dwpf, null as min_dwpf,
                null as min_feel, null as avg_feel, null as max_feel,
                null as max_gust_localts, null as max_drct,
                null as avg_sknt, null as vector_avg_drct,
                null as min_rstage, null as max_rstage,
                temp_hour, geom, id, name
                from {table} s JOIN stations t
                on (s.station = t.id)
                WHERE t.network = :network {sl} {dl}
                ORDER by day ASC, station ASC
                """,
                    table=table,
                    sl=sl,
                    dl=dl,
                ),
                conn,
                params=params,
                geom_col="geom",
            )  # type: ignore
    elif network.endswith("COCORAHS"):
        sl = " and id = :station " if station is not None else ""
        dl = ""
        if dt is not None:
            dl = " and day = :day "
        elif month is None and year is not None:
            dl = " and day >= :sts and day < :ets "
            params["sts"] = dateobj(year, 1, 1)
            params["ets"] = dateobj(year + 1, 1, 1)
        elif month is not None and year is not None:
            dl = " and day >= :sts and day < :ets "
            params["sts"] = dateobj(year, month, 1)
            params["ets"] = (
                dateobj(year, month, 1) + timedelta(days=35)
            ).replace(day=1)
        with get_sqlalchemy_conn("coop") as conn:
            df = gpd.read_postgis(
                sql_helper(
                    """
                SELECT id as station, to_char(day, 'YYYY-mm-dd') as date,
                precip, snow, snow_swe, snowd, snowd_swe, geom, id, name,
                null as max_dwpf, null as min_dwpf, null as max_tmpf,
                null as min_tmpf, null as max_rh, null as min_rh,
                null as max_gust, null as max_gust_localts,
                null as max_drct, null as avg_sknt, null as vector_avg_drct,
                null as max_feel, null as min_feel, null as avg_feel,
                extract(
                hour from obvalid at time zone t.tzname) as temp_hour
                from alldata_cocorahs s JOIN stations t
                on (s.iemid = t.iemid)
                WHERE t.network = :network {sl} {dl}
                ORDER by day ASC, id ASC
                """,
                    sl=sl,
                    dl=dl,
                ),
                conn,
                params=params,
                geom_col="geom",
            )  # type: ignore

    else:
        sl = " and id = :station " if station is not None else ""
        dl = ""
        table = "summary"
        if dt is not None:
            table = f"summary_{dt:%Y}"
            dl = " and day = :day "
        elif month is None and year is not None:
            table = f"summary_{year}"
        elif month is not None and year is not None:
            table = f"summary_{year}"
            dt2 = (dateobj(year, month, 1) + timedelta(days=35)).replace(day=1)
            params["sts"] = dateobj(year, month, 1)
            params["ets"] = dt2
            dl = " and day >= :sts and day < :ets "
        if table != "summary" and table < "summary_1900":
            raise HTTPException(404, detail="No data available for this date.")
        with get_sqlalchemy_conn("iem") as conn:
            df = gpd.read_postgis(
                sql_helper(
                    """
                SELECT id as station, to_char(day, 'YYYY-mm-dd') as date,
                max_tmpf, min_tmpf, pday as precip, max_gust, snow, snowd,
                min_rh, max_rh, max_dwpf, min_dwpf, min_feel,
                coalesce(avg_feel, (max_feel + min_feel) / 2.) as avg_feel,
                max_feel, max_drct,
                false as precip_est, false as tmpf_est,
                max_gust_ts at time zone t.tzname as max_gust_localts,
                to_char(coop_valid at time zone t.tzname, 'HH24') as temp_hour,
                avg_sknt, vector_avg_drct,
                min_rstage, max_rstage,
                geom, id, name
                from {table} s JOIN stations t on (s.iemid = t.iemid)
                WHERE t.network = :network {sl} {dl}
                ORDER by day ASC, id ASC
                """,
                    table=table,
                    sl=sl,
                    dl=dl,
                ),
                conn,
                params=params,
                geom_col="geom",
            )  # type:ignore
    return df


@router.get(
    "/daily.{fmt}",
    response_model=DailySchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(300)
def service(
    fmt: SupportedFormats,
    network: str = Query(
        ...,
        description="IEM Network Identifier",
        max_length=20,
        pattern="^[A-Z0-9_]+$",
    ),
    station: str = Query(
        None, description="IEM Station Identifier", max_length=20
    ),
    date: dateobj = Query(
        None,
        description="Local station calendar date",
        ge=dateobj(1900, 1, 1),
        le=dateobj(2030, 1, 1),
    ),
    month: int = Query(None, ge=1, le=12, description="Local station month"),
    year: int = Query(None, ge=1849, le=2030, description="Local station day"),
):
    """Replaced above with module __doc__"""
    if all(x is None for x in [station, date, month, year]):
        raise HTTPException(422, detail="Not enough arguments provided.")

    df = get_df(network, station, date, month, year)
    return deliver_df(df, fmt)


# Not really used
service.__doc__ = __doc__
