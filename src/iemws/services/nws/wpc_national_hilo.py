"""Weather Prediction Center Daily National High/Low Temperature.

The WPC issues a daily product that contains a contiguous US maximum and
minimum temperature.  The IEM attempts to archive this product and overwrite
intermediate values with what the public commonly sees for a national high and
low temperature report.

This service presently has three means to approach.

    - `year=2023` provide all the daily values for a year
    - `state=IA` provide all the values in the archive for the given state.
    - provide nothing and get the entire database.

There is no GeoJSON service yet as geo-locating the stations referenced is
not necessarily straight forward and not explicitly provide by the XTEUS
text/xml product.

The `n_x` return column/attribute denotes if the value is a minimum=N or
maximum=X.
"""

from datetime import date

import pandas as pd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper

from ...models import SupportedFormatsNoGeoJSON
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(state, year):
    """Handle the request, return dict"""
    limiter = []
    params = {"state": state}
    if state is not None:
        limiter.append("state = :state")
    if year is not None:
        limiter.append("date >= :sts and date <= :ets")
        params["sts"] = date(year, 1, 1)
        params["ets"] = date(year, 12, 31)

    # 1. We want anything issued between sts and valid
    # 2. We want anything issued < valid and expire > valid
    with get_sqlalchemy_conn("iem") as pgconn:
        df = pd.read_sql(
            sql_helper(
                """
            select to_char(date, 'YYYY-MM-DD') as date,
            station, name, state, n_x, value,
            to_char(sts at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ')
                as period_start,
            to_char(ets at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ')
                as period_end, product_id from wpc_national_high_low
            {where} {limitsql}
            ORDER by date asc
            """,
                where="where " if limiter else "",
                limitsql=" and ".join(limiter),
            ),
            pgconn,
            params=params,
            index_col=None,
        )  # type: ignore
    return df


@router.get(
    "/nws/wpc_national_hilo.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    state: str = Query(
        None,
        description="Two character state abbreviation",
        max_length=2,
    ),
    year: int = Query(None, description="Year to provide data for."),
):
    """Replaced above."""
    df = handler(state, year)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
