"""Storm Prediction Center Watch Outline.

This returns the issuance watch polygon outlines in GeoJSON format valid at
the provided UTC timestamp.  These are the four sided watch outlines and not
some county union.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Query
from geopandas import read_postgis
from pyiem.util import utc

from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def run(valid):
    """Do the work, please"""
    valid = utc() if valid is None else valid.replace(tzinfo=timezone.utc)
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = read_postgis(
            """
            SELECT sel,
            to_char(issued at time zone 'UTC',
                    'YYYY-MM-DDThh24:MI:SSZ') as utc_issued,
            to_char(expired at time zone 'UTC',
                    'YYYY-MM-DDThh24:MI:SSZ') as utc_expired,
            type, num, geom from watches WHERE
            issued <= %s and expired > %s
            ORDER by issued ASC
        """,
            pgconn,
            params=(valid, valid),
            index_col=None,
            geom_col="geom",
        )
    return df


@router.get(
    "/spc_watch_outline.geojson",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    valid: datetime = Query(None),
):
    """Replaced above."""
    df = run(valid)
    return deliver_df(df, "geojson")


service.__doc__ = __doc__
