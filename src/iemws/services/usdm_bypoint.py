"""US Drought Monitor (USDM) by lat/lon point.

The case of no-drought for the given USDM date is presented by a `null` value
in the JSON.
"""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query
from pandas.io.sql import read_sql
from pyiem.database import sql_helper

from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def run(sdate: date, edate: date, lon: float, lat: float):
    """Do the work, please"""

    with get_sqlalchemy_conn("postgis") as pgconn:
        df = read_sql(
            sql_helper("""
            with timedomain as (
                select distinct valid from usdm WHERE valid >= :sdate and
                valid <= :edate
            ),
            hits as (
                SELECT valid, max(dm) as category from usdm WHERE
                ST_Contains(geom, ST_Point(:lon, :lat,4326))
                and valid >= :sdate and valid <= :edate
                GROUP by valid
            )
            select to_char(t.valid, 'YYYY-mm-dd') as valid, category
            from timedomain t LEFT JOIN hits h on (t.valid = h.valid)
            ORDER by t.valid ASC
            """),
            pgconn,
            params={"sdate": sdate, "edate": edate, "lon": lon, "lat": lat},
            index_col=None,
        )
    df["category"] = df["category"].astype("Int64")
    return df


@router.get(
    "/usdm_bypoint.json",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def usdm_bypoint_service(
    sdate: Annotated[date, Query(description="start date")],
    edate: Annotated[date, Query(description="end date")],
    lon: Annotated[float, Query(description="Longitude degrees E")],
    lat: Annotated[float, Query(description="Latitude degrees E")],
):
    """Replaced above."""
    df = run(sdate, edate, lon, lat)
    return deliver_df(df, "json")


usdm_bypoint_service.__doc__ = __doc__
