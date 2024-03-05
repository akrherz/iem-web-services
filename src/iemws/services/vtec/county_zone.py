"""NWS VTEC Watch/Warning/Advisories (WaWA) by County/Zone.

This service provides VTEC-enabled NWS Watch, Warnings, and Advisories that
are specific to counties/parishes and zones.  There is a seperate service
for storm based / polygon VTEC events.  For the GeoJSON output, the returned
geometries are not the greatest due to a whole host of reasons.  The
attributes do contain the 6-character UGC codes, so joining that to a
higher resolution dataset may be necessary for your visualization purposes.

"""

from datetime import datetime, timezone

from fastapi import APIRouter, Query
from geopandas import read_postgis
from pyiem.nws.vtec import NWS_COLORS, get_ps_string
from pyiem.util import utc

from ...models import SupportedFormats
from ...models.county_zone import CountyZoneSchema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(valid):
    """Handler"""
    if valid is None:
        valid = utc()
    valid = valid.replace(tzinfo=timezone.utc)

    with get_sqlalchemy_conn("postgis") as pgconn:
        # Oh boy, the concept of "valid" is tough for in the future watches.
        df = read_postgis(
            """
            SELECT
            product_issue at time zone 'UTC' as utc_product_issue,
            issue at time zone 'UTC' as utc_issue,
            expire at time zone 'UTC' as utc_expire,
            w.phenomena || '.' || w.significance as ph_sig,
            w.wfo, eventid, phenomena, significance, w.ugc,
            null as nws_color, null as event_label,
            u.simple_geom as geom
            from warnings w JOIN ugcs u on (w.gid = u.gid) WHERE
            w.product_issue <= %s and w.expire > %s
            ORDER by w.product_issue ASC
            """,
            pgconn,
            geom_col="geom",
            params=(valid, valid),
            index_col=None,
        )
    df["nws_color"] = df["ph_sig"].apply(NWS_COLORS.get)
    df["event_label"] = df["ph_sig"].apply(
        lambda x: get_ps_string(*x.split("."))
    )
    return df


@router.get(
    "/vtec/county_zone.{fmt}",
    description=__doc__,
    response_model=CountyZoneSchema,
    tags=[
        "vtec",
    ],
)
def service(
    fmt: SupportedFormats,
    valid: datetime = Query(
        None, description="Return events that are valid at this time."
    ),
):
    """Replaced above."""
    df = handler(valid)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
