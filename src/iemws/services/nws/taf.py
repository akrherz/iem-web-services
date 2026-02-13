"""Terminal Aerodome Forecast (TAF) Single Forecast.

This service returns the forecast data found within a single TAF issuance
for one specified station/airport.  If you do not specify a `issued`
timestamp, the service returns the most recently issued TAF.  If you
specify a `issued` timestamp that does not exactly match a TAF
issuance, a search is done for the nearest issuance backward in time up
to 24 hours.  For example, providing `issued=2021-04-16T12:00Z` would provide
either the forecast issued at that time or the most recent forecast issued
prior to that time.

Presently, the `presentwx`, `skyl`, and `skyc` fields are arrays in JSON and
space seperated strings in TXT output formats.
"""

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Query, Response
from pandas.io.sql import read_sql
from pyiem.database import sql_helper
from pyiem.util import utc

# Local
from ...models import SupportedFormatsNoGeoJSON
from ...reference import MEDIATYPES
from ...util import get_sqlalchemy_conn

ISO = "YYYY-MM-DDThh24:MI:SSZ"
router = APIRouter()


def handler(fmt: str, station: str, issued: datetime | None):
    """Handle the request, return dict"""
    if issued is None:
        issued = utc()
    if issued.tzinfo is None:
        issued = issued.replace(tzinfo=timezone.utc)
    with get_sqlalchemy_conn("asos") as pgconn:
        df = read_sql(
            sql_helper(
                """
            WITH forecast as (
                select id, station, product_id, is_amendment from taf
                where station = :station and
                valid > :issued - '24 hours'::interval and valid <= :issued
                ORDER by valid DESC LIMIT 1)
            select
            to_char(t.valid at time zone 'UTC', :iso) as utc_valid,
            raw,
            case when t.ftype = 2 then true else false end as is_tempo,
            to_char(t.end_valid at time zone 'UTC', :iso) as utc_end_valid,
            sknt,
            drct,
            gust,
            visibility,
            presentwx,
            skyc,
            skyl,
            ws_level,
            ws_drct,
            ws_sknt,
            label as ftype,
            f.is_amendment,
            f.product_id,
            f.station
            from {table} t JOIN forecast f on
            (t.taf_id = f.id)
            JOIN taf_ftype ft on (t.ftype = ft.ftype)
            ORDER by valid ASC
            """,
                table=f"taf{issued:%Y}",
            ),
            pgconn,
            params={"station": station, "issued": issued, "iso": ISO},
            index_col=None,
        )

    if fmt == "txt":
        for col in ["presentwx", "skyc", "skyl"]:
            df[col] = [" ".join(map(str, item)) for item in df[col]]
        return df.to_csv(index=False)
    return df.to_json(orient="table", index=False)


@router.get(
    "/nws/taf.{fmt}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    station: Annotated[str, Query(min_length=4, max_length=4)],
    issued: Annotated[
        datetime | None,
        Query(description="Valid time to look for most recent TAF before"),
    ] = None,
):
    """Replaced above."""
    return Response(handler(fmt, station, issued), media_type=MEDIATYPES[fmt])


service.__doc__ = __doc__
