"""IEM Trending Autoplots.

Returns the mostly frequently requested IEM Autoplots over the past six hours,
give or take some caching.
"""

import pandas as pd
from fastapi import APIRouter

from ...models import SupportedFormatsNoGeoJSON
from ...util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler():
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("mesosite") as pgconn:
        df = pd.read_sql(
            "select appid, count(*) from autoplot_timing where "
            "valid > now() - '6 hours'::interval "
            "GROUP by appid order by count desc limit 10",
            pgconn,
            index_col=None,
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
