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

# Third Party
from pandas.io.sql import read_sql
from fastapi import Response, Query, APIRouter
from pyiem.util import utc

# Local
from ...models import SupportedFormatsNoGeoJSON
from ...reference import MEDIATYPES
from ...util import get_dbconn

ISO = "YYYY-MM-DDThh24:MI:SSZ"
router = APIRouter()


def handler(fmt, station, issued):
    """Handle the request, return dict"""
    pgconn = get_dbconn("asos")
    if issued is None:
        issued = utc()
    if issued.tzinfo is None:
        issued = issued.replace(tzinfo=timezone.utc)

    df = read_sql(
        f"""
        WITH forecast as (
            select id from taf where station = %s and
            valid > %s - '24 hours'::interval and valid <= %s
            ORDER by valid DESC LIMIT 1)
        select
        to_char(t.valid at time zone 'UTC', '{ISO}') as utc_valid,
        raw,
        is_tempo,
        to_char(t.end_valid at time zone 'UTC', '{ISO}') as utc_end_valid,
        sknt,
        drct,
        gust,
        visibility,
        presentwx,
        skyc,
        skyl,
        ws_level,
        ws_drct,
        ws_sknt
        from taf{issued.year} t JOIN forecast f on
        (t.taf_id = f.id) ORDER by valid ASC
        """,
        pgconn,
        params=(station, issued, issued),
        index_col=None,
    )

    if fmt == "txt":
        for col in ["presentwx", "skyc", "skyl"]:
            df[col] = [" ".join(map(str, item)) for item in df[col]]
        return df.to_csv(index=False)
    if fmt == "json":
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
    station: str = Query(..., min_length=4, max_length=4),
    issued: datetime = Query(None),
):
    """Replaced above."""
    return Response(handler(fmt, station, issued), media_type=MEDIATYPES[fmt])


service.__doc__ = __doc__
