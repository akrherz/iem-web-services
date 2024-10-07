"""SPC/WPC Outlooks by Point.

This service emits the Storm Prediction Center (SPC) and Weather Prediction
Center (WPC) outlooks at a given point and optional valid time.  This service
covers the SPC Convective, SPC Fire Weather, and WPC Excessive Rainfall
Outlook.

The meaning of `valid` for this service is to specify that point in time that
the given outlooks would have been in place.  For example, a valid time of
`2011-04-27 18:00+00` means you would get the most recent day 1 outlooks at
that time and then the most recent day 2-8 outlooks valid for subsequent dates.
Rewording, the day 2 outlook returned for the `2011-04-27` provided date is
valid for the next day and not the day 2 outlook from "yesterday".

If `valid` is not provided, you get the current outlooks.

The SPC hatched/significant probability is handled in a special manner such
that both the `SIGN` threshold and outlook probability number are both
returned.
"""

from datetime import datetime, timedelta, timezone

import geopandas as gpd
from fastapi import APIRouter, Query
from pyiem.util import utc
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...models.nws.outlook_by_point import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(lon, lat, valid):
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            text(
                """
            with opts as (
                select product_issue, issue, expire, day, id, outlook_type,
                rank() OVER (PARTITION by day, outlook_type
                    ORDER by product_issue desc) from spc_outlook
                where product_issue > :sts and product_issue < :valid),
            current as (
                select * from opts where rank = 1),
            agg as (
                select geom, day, outlook_type,
                product_issue at time zone 'UTC' as product_issue,
                issue at time zone 'UTC' as issue,
                expire at time zone 'UTC' as expire,
                o.threshold, rank() OVER (PARTITION by day, outlook_type,
                category
                    ORDER by case when o.threshold = 'SIGN'
                    then 0 else priority end desc), priority, category
                from spc_outlook_geometries o, current c,
                spc_outlook_thresholds t
                WHERE o.threshold = t.threshold and o.spc_outlook_id = c.id and
                ST_Contains(geom, ST_Point(:lon, :lat, 4326)))
            SELECT * from agg where rank = 1 or threshold = 'SIGN';
            """
            ),
            pgconn,
            geom_col="geom",
            params={
                "valid": valid,
                "sts": valid - timedelta(hours=27),
                "lat": lat,
                "lon": lon,
            },
            index_col=None,
        )
    return df.drop(columns=["rank", "priority"], errors="ignore")


@router.get(
    "/nws/outlook_by_point.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormats,
    lon: float = Query(..., description="Longitude (deg E)"),
    lat: float = Query(..., description="Latitude (deg N)"),
    valid: datetime = Query(None, description="Outlooks valid at UTC time."),
):
    """Replaced above."""
    if valid is None:
        valid = utc()
    else:
        valid = valid.replace(tzinfo=timezone.utc)
    df = handler(lon, lat, valid)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
