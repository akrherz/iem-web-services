"""NWS VTEC Point in Time Events Status.

This service provides a listing of NWS VTEC events that are active at a
given point in time.  GeoJSON is presently not supported for this service.
"""
from datetime import datetime, timezone

import pandas as pd
from fastapi import APIRouter, Query
from pyiem.nws.vtec import NWS_COLORS, get_ps_string
from sqlalchemy import text

from ...models import SupportedFormatsNoGeoJSON
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(valid, wfo):
    """Handler"""
    params = {"valid": valid, "wfo": wfo}
    wfolimiter = ""
    if wfo is not None:
        wfolimiter = " and w.wfo = :wfo "
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = pd.read_sql(
            text(
                f"""
            select w.wfo, eventid, phenomena, significance,
            phenomena || '.' || significance as ph_sig,
            string_agg(u.name || ' ['||u.state||']', ', ') as locations,
            max(updated at time zone 'UTC') as updated,
            min(issue at time zone 'UTC') as issue,
            max(expire at time zone 'UTC') as expire,
            max(hvtec_nwsli) as nwsli,
            max(purge_time at time zone 'UTC') as product_expires,
            max(fcster) as fcster
            from warnings w JOIN ugcs u on (w.gid = u.gid)
            where expire >= :valid and
            product_issue <= :valid {wfolimiter}
            GROUP by w.wfo, eventid, phenomena, significance, ph_sig
            ORDER by w.wfo, updated desc
            """
            ),
            pgconn,
            params=params,
            index_col=None,
        )
    df["nws_color"] = df["ph_sig"].apply(NWS_COLORS.get)
    df["event_label"] = df["ph_sig"].apply(
        lambda x: get_ps_string(*x.split("."))
    )
    if df.empty:
        df["issue"] = pd.to_datetime([])
    df["url"] = (
        "https://mesonet.agron.iastate.edu/vtec/#"
        + df["issue"].dt.strftime("%Y")
        + "-O-NEW-K"
        + df["wfo"]
        + "-"
        + df["phenomena"]
        + "-"
        + df["significance"]
        + "-"
        + df["eventid"].astype(str).str.pad(4, fillchar="0")
    )
    return df


@router.get(
    "/vtec/events_status.{fmt}",
    description=__doc__,
    tags=[
        "vtec",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    valid: datetime = Query(
        None, description="UTC timestamp for status, defaults to now."
    ),
    wfo: str = Query(
        None, description="WFO 3-letter code for filter.", max_length=3
    ),
):
    """Replaced above."""
    if valid is None:
        valid = datetime.utcnow()
    valid = valid.replace(tzinfo=timezone.utc)
    df = handler(valid, wfo)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
