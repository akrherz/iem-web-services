"""IEM Trending Autoplots.

This service returns the top 10 trending autoplot applications by comparing
a ratio of requests over the past six hours vs the previous 6 hours.  Very
unscientific, but it is a start.
"""

import pandas as pd
from fastapi import APIRouter
from pyiem.database import sql_helper
from pyiem.util import utc

from ...models import SupportedFormatsNoGeoJSON
from ...util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler():
    """Handle the request, return dict"""
    p3 = utc()
    p2 = p3 - pd.Timedelta("6 hours")
    p1 = p2 - pd.Timedelta("6 hours")
    params = {
        "p1": p1,
        "p2": p2,
        "p3": p3,
    }
    with get_sqlalchemy_conn("mesosite") as pgconn:
        df = pd.read_sql(
            sql_helper(
                """
            with one as (
                 select appid, count(*) from autoplot_timing where
                 valid between :p1 and :p2 group by appid),
            two as (
                 select appid, count(*) from autoplot_timing where
                 valid between :p2 and :p3 group by appid)
            select t.appid, t.count
            from one o JOIN two t on (o.appid = t.appid)
            order by (t.count - o.count) / o.count::float desc
            LIMIT 10
            """
            ),
            pgconn,
            index_col=None,
            params=params,
        )
    return df


@router.get(
    "/iem/trending_autoplots.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(600)
def service(
    fmt: SupportedFormatsNoGeoJSON,
):
    """Replaced above."""
    return deliver_df(handler(), fmt)


service.__doc__ = __doc__
