"""NWS Flash Flood + Tornado Emergencies.

The IEM attempts to keep track of NWS issued Flash Flood and
Tornado Warnings which are specially denoted as emergencies.  This listing is
__not__ official!

For GeoJSON, this service will return a mixture of storm based warning polygons
and county polygons.  The `is_sbw` field will denote which is which.  The
reason is that some of these emergencies predated polygon warnings.
"""

import geopandas as gpd
from fastapi import APIRouter
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...models.nws.emergencies import Schema
from ...util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler():
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            text(
                """
            with county as (
                select w.wfo, eventid, phenomena, significance,
                vtec_year,
                min(product_issue at time zone 'UTC') as utc_product_issue,
                min(init_expire at time zone 'UTC') as utc_init_expire,
                min(issue at time zone 'UTC') as utc_issue,
                max(expire at time zone 'UTC') as utc_expire,
                array_to_string(array_agg(distinct substr(w.ugc, 1, 2)), ',')
                    as states, st_union(simple_geom) as geo
                from warnings w JOIN ugcs u on (w.gid = u.gid)
                where phenomena in ('TO', 'FF') and significance = 'W' and
                is_emergency
                GROUP by w.wfo, eventid, phenomena, significance, vtec_year),
            polys as (
                select wfo, eventid, phenomena,
                vtec_year, polygon_begin, geom
                from sbw where phenomena in ('TO', 'FF')
                and significance = 'W' and is_emergency)
            select c.vtec_year as year, c.wfo, c.eventid, c.phenomena,
            c.significance,
            c.utc_product_issue, c.utc_init_expire, c.utc_issue, c.utc_expire,
            c.states, coalesce(p.geom, c.geo) as geom,
            case when p.wfo is not null then 't' else 'f' end as is_sbw
            from county c LEFT JOIN polys p on
            (c.wfo = p.wfo and c.eventid = p.eventid and
            c.phenomena = p.phenomena and c.vtec_year = p.vtec_year)
            ORDER by c.utc_issue asc, p.polygon_begin asc
            """
            ),
            pgconn,
            geom_col="geom",
            index_col=None,
        )
    # NOTE the above has duplicated entries, so we 'dedup'
    df = (
        df.groupby(["year", "wfo", "eventid", "phenomena", "significance"])
        .first()
        .reset_index()
        .sort_values("utc_issue", ascending=True)
    )
    df["uri"] = (
        "/vtec/event/"
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
    "/nws/emergencies.{fmt}",
    description=__doc__,
    tags=[
        "vtec",
    ],
    response_model=Schema,
)
def service(
    fmt: SupportedFormats,
):
    """Replaced above."""
    df = handler()
    return deliver_df(df, fmt)


service.__doc__ = __doc__
