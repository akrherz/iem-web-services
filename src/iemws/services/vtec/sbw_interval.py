"""NWS VTEC Storm Based Warnings over Time Interval.

This service provides storm based warnings over a given time interval.  The
default methodology is to include any events that were **issued** between the
inclusive `sdate` and `edate` UTC timestamps.  For some storm based warnings,
the associated geometry can be shrinked with event updates.  The default
option is to only provide the issuance geometry.

The `only_new` option deserves some explanation.  The default behavior is to
only return events that have a status of `NEW`.  This is the default behavior
for the NWS VTEC service.  This `NEW` status is associated with the initial
issuance of the product.  Some events get updated with polygon geometries that
may be shrunk.  The `utc_polygon_begin` and `utc_polygon_end` timestamps
explicitly track the time duration of the polygon, whereas the `utc_issue`
and `utc_expire` track the time duration of the event, but are not always
set for the polygon updates.  I realize this is horribly confusing!  Attempting
to simplify, if you are only worried about the issuance polygons, use the
`utc_{issue,expire}` timestamps.  If you are worried about the polygon
updates, use the `utc_polygon_{begin,end}` timestamps.
"""

# stdlib
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Query

# Third party
from geopandas import read_postgis
from pyiem.nws.vtec import NWS_COLORS, get_ps_string
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...models.sbw_interval import SBWIntervalModel
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(begints, endts, wfo, only_new, ph):
    """Handler"""
    begints = begints.replace(tzinfo=timezone.utc)
    endts = endts.replace(tzinfo=timezone.utc)

    params = {"begints": begints, "endts": endts}
    wfolimiter = ""
    statuslimiter = ""
    phlimiter = ""
    if ph is not None:
        params["ph"] = ph
        phlimiter = "AND phenomena = ANY(:ph) "
    if wfo is not None:
        params["wfo"] = wfo
        wfolimiter = " and wfo = ANY(:wfo) "
    if only_new:
        statuslimiter = " and status = 'NEW' "
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = read_postgis(
            text(
                f"""
            SELECT
            issue at time zone 'UTC' as utc_issue,
            expire at time zone 'UTC' as utc_expire,
            polygon_begin at time zone 'UTC' as utc_polygon_begin,
            polygon_end at time zone 'UTC' as utc_polygon_end,
            w.phenomena || '.' || w.significance as ph_sig,
            w.wfo, eventid, phenomena, significance, null as nws_color,
            null as event_label, status, geom
            from sbw w WHERE
            w.polygon_begin >= :begints and w.polygon_begin < :endts
            {wfolimiter} {statuslimiter} {phlimiter}
            ORDER by w.polygon_begin ASC
            """
            ),
            pgconn,
            geom_col="geom",
            params=params,
            index_col=None,
        )
    df["nws_color"] = df["ph_sig"].apply(NWS_COLORS.get)
    df["event_label"] = df["ph_sig"].apply(
        lambda x: get_ps_string(*x.split("."))
    )
    return df


@router.get(
    "/vtec/sbw_interval.{fmt}",
    description=__doc__,
    response_model=SBWIntervalModel,
    tags=[
        "vtec",
    ],
)
def service(
    fmt: SupportedFormats,
    begints: datetime = Query(
        ..., description="Inclusive UTC timestamp window start for issuance."
    ),
    endts: datetime = Query(
        ..., description="Exclusive UTC timestamp window end for issuance."
    ),
    wfo: List[str] = Query(
        None, description="WFO 3-letter codes for filter.", max_length=3
    ),
    only_new: bool = Query(True, description="Only include issuance events."),
    ph: List[str] = Query(
        None, description="VTEC Phenomena 2-letter codes.", max_length=2
    ),
):
    """Replaced above."""
    df = handler(begints, endts, wfo, only_new, ph)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
