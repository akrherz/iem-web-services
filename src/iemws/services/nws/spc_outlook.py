"""Storm Prediction Center Convective/Fire Outlook.

This service returns a single SPC outlook, either fire weather or convective,
for a given date and issuance cycle.  Both the date and issuance cycle are a
bit tricky to explain, so we shall attempt to do so here.

The `valid` param is the date the outlook is valid for, but since an individual
outlook crosses calendar dates, this is the date of the first calendar date
in the period.  For example, the Day 1 outlook issued on 20 Jun 2022 at 12 UTC
is valid for a period ending at 12 UTC on 21 Jun 2022.  The `valid` value in
this case is 20 Jun 2022.

Next up is the `cycle` parameter, which the IEM attempts to compute to ensure
that there is one canonical outlook per issuance cycle.  For example, if SPC
issues a 20 UTC outlook and then updates it 30 minutes later, that update
then is assigned as the canonical update at 20 UTC for that date.  This is not
an exact science, so caveat emptor.  So you are likely wondering what these
cycle values are?  They are the UTC hour timestamp of the outlook, for example
1, 6, 13, 16, and 20 are the possible values for the Day 1 convective outlook.

And finally, the `outlook_type` parameter, which is either `C` for convective
or `F` for fire weather.
"""
from datetime import date, timedelta

import geopandas as gpd
from fastapi import APIRouter, Query
from pyiem.util import utc
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(day, valid, cycle, outlook_type):
    """Handle the request, return dict"""
    # Maybe brittle
    expire = utc(valid.year, valid.month, valid.day, 12) + timedelta(days=1)
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            text(
                """
            select
            issue at time zone 'UTC' as issue,
            product_issue at time zone 'UTC' as product_issue,
            expire at time zone 'UTC' as expire,
            threshold, category, geom, product_id
            from spc_outlooks
            where outlook_type = :outlook_type and cycle = :cycle
            and expire = :expire and day = :day
            """
            ),
            pgconn,
            geom_col="geom",
            params={
                "outlook_type": outlook_type,
                "cycle": cycle,
                "expire": expire,
                "day": day,
            },
            index_col=None,
        )
    return df


@router.get(
    "/nws/spc_outlook.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormats,
    day: int = Query(..., description="Day 1-8 Outlook Value"),
    valid: date = Query(..., description="SPC Outlook Date"),
    cycle: int = Query(..., description="SPC Outlook Cycle"),
    outlook_type: str = Query(
        "C",
        description="SPC Outlook Type",
        enum=["C", "F"],
    ),
):
    """Replaced above."""
    df = handler(day, valid, cycle, outlook_type)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
