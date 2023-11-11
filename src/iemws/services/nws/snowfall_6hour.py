"""NWS Six Hour Snowfall Reports.

Via mostly paid and some volunteer reports, the NWS collects six hour
snowfall totals from a very limited number of locations.  These observations
are made at 00, 06, 12, and 18 UTC.  This service provides access to these
reports as collected via disseminated SHEF reports using the SFQ code.

Trace values are encoded as ``0.0001``.
"""
from datetime import datetime

import geopandas as gpd
from fastapi import APIRouter, Query
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...models.nws.snowfall_6hour import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(valid):
    """Handle the request, return dict"""
    params = {
        "valid": valid,
    }
    with get_sqlalchemy_conn("hads") as pgconn:
        df = gpd.read_postgis(
            text(
                """
            select station, network, value, geom, wfo, ugc_county,
            valid at time zone 'UTC' as utc_valid,
            st_x(geom) as longitude, st_y(geom) as latitude, name, state from
            raw r, stations t where valid = :valid and key = 'SFQRZZZ'
            and r.station = t.id and
            (t.network ~* 'COOP' or t.network ~* 'DCP')
            order by station asc, network asc
            """
            ),
            pgconn,
            geom_col="geom",
            params=params,
            index_col=None,
        )
        # We have duplicate network entries, so we do a hack
        if not df.empty:
            df = df.groupby("station").first()
    return df


@router.get(
    "/nws/snowfall_6hour.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormats,
    valid: datetime = Query(
        ..., description="UTC Timestamp to return reports for."
    ),
):
    """Replaced above."""
    df = handler(valid)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
