"""Exposes Iowa DOT 'Dashcam' imagery from its snowplows."""

from datetime import datetime, timedelta, timezone

import geopandas as gpd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper
from pyiem.util import utc

from ..models import SupportedFormats
from ..models.idot_dashcam import IDOTDashcamSchema
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def make_url(row):
    """Convert into web address."""
    return row["utc_valid"].strftime(
        "https://mesonet.agron.iastate.edu/archive/data/%Y/%m/%d/"
        f"camera/idot_trucks/{row['cid']}/{row['cid']}_%Y%m%d%H%M.jpg"
    )


def handler(valid, window):
    """Do the requested work."""
    with get_sqlalchemy_conn("postgis") as pgconn:
        df = gpd.read_postgis(
            sql_helper("""
    SELECT row_number() OVER() as index, label as cid,
    valid as utc_valid, geom, ST_X(geom) as lon,
    ST_Y(geom) as lat from idot_dashcam_log WHERE valid >= :sts
    and valid <= :ets ORDER by valid ASC"""),
            pgconn,
            params={
                "sts": valid - timedelta(minutes=window),
                "ets": valid + timedelta(minutes=window),
            },
            geom_col="geom",
            index_col=None,
        )  # type: ignore
    if not df.empty:
        df["imgurl"] = df.apply(make_url, axis=1)
        df["utc_valid"] = df["utc_valid"].dt.strftime("%Y-%m-%dT%H:%MZ")
    else:
        df["imgurl"] = ""
    return df


@router.get(
    "/idot_dashcam.{fmt}",
    response_model=IDOTDashcamSchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
def idot_dashcam_service(
    fmt: SupportedFormats,
    valid: datetime = Query(
        None, description="UTC timestamp to look for imagery."
    ),
    window: int = Query(
        15,
        description=("Number of minutes to look around the given valid."),
    ),
):
    """Replaced Below."""
    if valid is None:
        valid = utc() - timedelta(minutes=window * 2)
    if valid.tzinfo is None:
        valid = valid.replace(tzinfo=timezone.utc)
    df = handler(valid, window)
    return deliver_df(df, fmt)


idot_dashcam_service.__doc__ = __doc__
