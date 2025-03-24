"""NWS VTEC Point in Time Events Status.

This service provides a listing of NWS VTEC events that are active at a
given point in time.  GeoJSON is presently not supported for this service.

The `issue_product_id` and `last_product_id` values are IEM assigned NWS Text
Product Identifiers that can be used to retrieve the raw text.  You can either
get that text via this API at `/nwstext/{product_id}` or take the user to a
more friendly interface at
`https://mesonet.agron.iastate.edu/p.php?pid={product_id}`.
"""

from datetime import datetime, timezone

import pandas as pd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper
from pyiem.nws.vtec import NWS_COLORS, get_ps_string
from pyiem.util import utc

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
            sql_helper(
                """
            with data as (
                select w.wfo, eventid, phenomena, significance,
                phenomena || '.' || significance as ph_sig,
                vtec_year as year,
                status,
                u.name || ' ['||u.state||']' as location,
                updated at time zone 'UTC' as _updated,
                issue at time zone 'UTC' as _issue,
                expire at time zone 'UTC' as _expire,
                hvtec_nwsli as nwsli,
                purge_time at time zone 'UTC' as product_expires,
                fcster as fcster,
                product_ids[1] as issue_product_id,
                product_ids[array_upper(product_ids, 1)] as last_product_id
                from warnings w JOIN ugcs u on (w.gid = u.gid)
                where expire >= :valid and
                product_issue <= :valid {wfolimiter}
            )
            SELECT wfo, eventid, phenomena, significance, ph_sig, year,
            max(status) as status,
            string_agg(location, ', ') as locations,
            max(_updated) as updated,
            min(_issue) as issue,
            max(_expire) as expire,
            max(nwsli) as nwsli,
            max(product_expires) as product_expires,
            max(fcster) as fcster,
            min(issue_product_id) as issue_product_id,
            max(last_product_id) as last_product_id
            from data
            GROUP by
                wfo, eventid, phenomena, significance, ph_sig, year, status
            ORDER by wfo, updated desc
            """,
                wfolimiter=wfolimiter,
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
        + df["year"].astype(str)
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
    valid = utc() if valid is None else valid.replace(tzinfo=timezone.utc)
    df = handler(valid, wfo)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
