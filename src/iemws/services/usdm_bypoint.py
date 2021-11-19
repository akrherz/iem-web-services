"""US Drought Monitor (USDM) by lat/lon point.

The case of no-drought for the given USDM date is presented by a `null` value
in the JSON.
"""
from datetime import date

from pandas.io.sql import read_sql
from fastapi import Query, APIRouter
from ..util import get_dbconn, deliver_df

ISO = "%Y-%m-%dT%H:%M:%SZ"
router = APIRouter()


def run(sdate, edate, lon, lat):
    """Do the work, please"""
    pgconn = get_dbconn("postgis")

    df = read_sql(
        """
        with timedomain as (
            select distinct valid from usdm WHERE valid >= %s and
            valid <= %s
        ),
        hits as (
            SELECT valid, max(dm) as category from usdm WHERE
            ST_Contains(
                geom, ST_SetSRID(ST_GeomFromEWKT('POINT(%s %s)'),4326))
            and valid >= %s and valid <= %s
            GROUP by valid
        )
        select to_char(t.valid, 'YYYY-mm-dd') as valid, category
        from timedomain t LEFT JOIN hits h on (t.valid = h.valid)
        ORDER by t.valid ASC
        """,
        pgconn,
        params=(sdate, edate, lon, lat, sdate, edate),
        index_col=None,
    )
    df["category"] = df["category"].astype("Int64")
    return df


@router.get("/usdm_bypoint.json", description=__doc__)
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
