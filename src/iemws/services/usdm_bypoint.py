"""US Drought Monitor (USDM) by lat/lon point.

The case of no-drought for the given USDM date is presented by a `null` value
in the JSON.
"""
from datetime import date

from fastapi import APIRouter, Query
from pandas.io.sql import read_sql

from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def run(sdate, edate, lon, lat):
    """Do the work, please"""

    giswkt = f"POINT({lon} {lat})"
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = read_sql(
            """
            with timedomain as (
                select distinct valid from usdm WHERE valid >= %s and
                valid <= %s
            ),
            hits as (
                SELECT valid, max(dm) as category from usdm WHERE
                ST_Contains(
                    geom, ST_SetSRID(ST_GeomFromEWKT(%s),4326))
                and valid >= %s and valid <= %s
                GROUP by valid
            )
            select to_char(t.valid, 'YYYY-mm-dd') as valid, category
            from timedomain t LEFT JOIN hits h on (t.valid = h.valid)
            ORDER by t.valid ASC
            """,
            pgconn,
            params=(sdate, edate, giswkt, sdate, edate),
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
    sdate: date = Query(...),
    edate: date = Query(...),
    lon: float = Query(...),
    lat: float = Query(...),
):
    """Replaced above."""
    df = run(sdate, edate, lon, lat)
    return deliver_df(df, "json")


usdm_bypoint_service.__doc__ = __doc__
