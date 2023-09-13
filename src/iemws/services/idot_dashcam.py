"""Exposes Iowa DOT 'Dashcam' imagery from its snowplows."""
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query
from geopandas import read_postgis

# third party
from pyiem.util import utc

from ..models import SupportedFormats
from ..models.idot_dashcam import IDOTDashcamSchema

# Local
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
        df = read_postgis(
            "SELECT row_number() OVER() as index, label as cid, "
            "valid as utc_valid, geom, ST_X(geom) as lon, "
            "ST_Y(geom) as lat from idot_dashcam_log WHERE valid >= %s "
            "and valid <= %s ORDER by valid ASC",
            pgconn,
            params=(
                valid - timedelta(minutes=window),
                valid + timedelta(minutes=window),
            ),
            geom_col="geom",
            index_col=None,
        )
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
