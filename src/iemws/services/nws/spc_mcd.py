"""Storm Prediction Center Mesoscale Convective Discussions.

This service either returns any MCDs that have a valid time at the given
valid time.  Rewording, if the issuance time is before the given time and the
expiration is after the given time.

The other option is to provide a number of hours to look back for any
MCDs issued within that timespan. For example, to get any MCDs issued
within the past six hours `/api/1/nws/spc_mcd.geojson?hours=6`.
"""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query
from geopandas import read_postgis
from pyiem.util import utc

from ...models import SupportedFormats
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(valid, hours):
    """Handle the request, return dict"""
    if valid is None:
        valid = utc()
    valid = valid.replace(tzinfo=timezone.utc)
    if hours is not None:
        params = (valid - timedelta(hours=hours), valid)
        limiter = "issue >= %s and issue <= %s"
    else:
        params = (valid, valid)
        limiter = "issue <= %s and expire >= %s"

    # 1. We want anything issued between sts and valid
    # 2. We want anything issued < valid and expire > valid
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = read_postgis(
            f"""
            select product_id, geom, year, num,
            issue at time zone 'UTC' as issue,
            expire at time zone 'UTC' as expire,
            watch_confidence, concerning from mcd
            where {limiter}
            """,
            pgconn,
            geom_col="geom",
            params=params,
            index_col=None,
        )
    return df


@router.get(
    "/nws/spc_mcd.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormats,
    valid: datetime = Query(
        None, description="Return MCDs at valid time, default is now"
    ),
    hours: int = Query(
        None, description="Return MCDs issued given hours prior to valid time"
    ),
):
    """Replaced above."""
    df = handler(valid, hours)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
