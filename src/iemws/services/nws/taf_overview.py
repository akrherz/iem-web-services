"""Terminal Aerodome Forecast (TAF) Overview.

This service provides metadata on available TAF forecasts within the
IEM database.  The attributes for each
forecast include references to API endpoints providing either the raw
text (`text_href`) or JSON representation (`data_href`).  Additionally a
forecast aggregate of `min_visibility` (miles) is provided.

If you provide a ``station``, you can request up to a year's worth of
TAF metadata.  If you provide no ``station``, you can request up to 10 days
worth of TAF metadata between ``sts`` and ``ets``.  If you provide ``at``,
then you will get the last TAF forecast for the provided station or all
stations prior to that time. The search looks over a 10 day window.

The ``seqnum`` value in the response is meant to order the forecasts for
a given station with `1` representing the most recent TAF for the station.
"""

from datetime import datetime, timedelta
from typing import Annotated

import geopandas as gpd
from fastapi import APIRouter, HTTPException
from pydantic import Field
from pyiem.database import sql_helper
from pyiem.util import utc

from ...models import SupportedFormats
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(
    station: str | None,
    sts: datetime | None,
    ets: datetime | None,
    at: datetime | None,
):
    """Handle the request."""
    query_params = {
        "station": station,
        "sts": sts,
        "ets": ets,
    }
    # 1. Provided station
    station_limiter = "" if station is None else " and station = :station"
    row_number_operator = "="
    # 2a. Provided at
    if at is not None:
        query_params["sts"] = at - timedelta(days=10)
        query_params["ets"] = at
    # 2b. sts and ets are provided
    elif sts is not None and ets is not None:
        row_number_operator = ">="
        if (ets - sts) > timedelta(days=366):
            raise HTTPException(
                status_code=422,
                detail="Too large of a request, please limit to 1 year",
            )
        if station is None and (ets - sts) > timedelta(days=10):
            raise HTTPException(
                status_code=422,
                detail="Too large of a request, please limit to 10 days",
            )
    # 3. No timestamps provided
    else:
        query_params["sts"] = utc() - timedelta(days=10)
        query_params["ets"] = utc()
    with get_sqlalchemy_conn("asos") as pgconn:
        df = gpd.read_postgis(
            sql_helper(
                """
            with forecasts as (
                select station, id, valid as issuance, product_id,
                is_amendment,
                row_number() OVER (PARTITION by station ORDER by valid DESC)
                from taf where valid > :sts and valid <= :ets
                {station_limiter}),
            agg as (
                select station, id, issuance, product_id, is_amendment,
                row_number as seqnumber
                from forecasts
                where row_number {row_number_operator} 1),
            stinfo as (
                select a.*, t.geom, t.name from agg a JOIN stations t on (
                    (case when substr(a.station, 1, 1) = 'K'
                    then substr(a.station, 2, 3) else a.station end) = t.id)
                WHERE t.network ~* 'ASOS'),
            agg2 as (
                select a.id, min(visibility) as min_visibility
                from agg a JOIN taf_forecast t on
                (a.id = t.taf_id) GROUP by a.id)

            select s.station, s.product_id, s.geom, s.name, s.is_amendment,
            s.seqnumber,
            to_char(s.issuance at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ')
                as utc_issued,
            ST_X(geom) as lon, ST_Y(geom) as lat,
            a.min_visibility from stinfo s JOIN agg2 a on (s.id = a.id)
            order by station asc, seqnumber asc
            """,
                station_limiter=station_limiter,
                row_number_operator=row_number_operator,
            ),
            pgconn,
            geom_col="geom",
            index_col=None,
            params=query_params,
        )  # type: ignore
    df["data_href"] = (
        "/api/1/nws/taf.json?station="
        + df["station"]
        + "&issued="
        + df["utc_issued"]
    )
    df["text_href"] = "/api/1/nwstext/" + df["product_id"].str.strip()
    return df


@router.get(
    "/nws/taf_overview.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormats,
    station: Annotated[
        str | None, Field(description="Four character ICAO")
    ] = None,
    sts: Annotated[
        datetime | None, Field(description="Start time for query")
    ] = None,
    ets: Annotated[
        datetime | None, Field(description="End time for query")
    ] = None,
    at: Annotated[
        datetime | None, Field(description="Time to query prior to")
    ] = None,
):
    """Replaced above."""
    df = handler(station, sts, ets, at)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
