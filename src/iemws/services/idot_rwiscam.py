"""Exposes Iowa RWIS Imagery."""
from datetime import datetime, timedelta, timezone

# third party
from pyiem.util import utc
import geopandas as gpd
from fastapi import Query, APIRouter

# Local
from ..util import deliver_df, get_dbconn
from ..models import SupportedFormats
from ..models.idot_rwiscam import IDOTRWIScamSchema

router = APIRouter()


def make_url(fullid, row):
    """Convert into web address."""
    return row["utc_valid"].strftime(
        "https://mesonet.agron.iastate.edu/archive/data/%Y/%m/%d/"
        f"camera/{fullid}/{fullid}_%Y%m%d%H%M.jpg"
    )


def handler(valid, window):
    """Do the requested work."""
    pgconn = get_dbconn("mesosite")
    df = gpd.read_postgis(
        """
        with data as (
            select cam, valid, valid - %s::timestamptz as delta, name, geom
            from camera_log c JOIN webcams w on (c.cam = w.id)
            where valid >= %s and valid <= %s and cam ~* 'IDOT-')
        select cam as fullid,
        substr(cam, 1, 8) as rwisid,
        substr(cam, 10, 2)::int as viewid,
        valid at time zone 'UTC' as utc_valid,
        case when delta < '0'::interval then -1 * delta else delta end
            as proximity, name, geom from data
        ORDER by proximity asc
        """,
        pgconn,
        params=(
            valid,
            valid - timedelta(minutes=window),
            valid + timedelta(minutes=window),
        ),
        geom_col="geom",
        index_col=None,
    )
    if not df.empty:
        # Take the first entry for each cam
        df = df.groupby("fullid").first().copy()
        # Now we need a different datastructure
        res = {}
        for fullid, row in df.iterrows():
            entry = res.setdefault(
                row["rwisid"],
                {
                    "utc_valid": row["utc_valid"].strftime("%Y-%m-%dT%H:%MZ"),
                    "geom": row["geom"],
                    "cid": row["rwisid"],
                    "imgurl9": None,  # May not be defined otherwise
                },
            )
            entry[f"imgurl{row['viewid']}"] = make_url(fullid, row)
        df = gpd.GeoDataFrame.from_dict(res, orient="index", geometry="geom")
    else:
        df["imgurl0"] = ""
    return df


@router.get(
    "/idot_rwiscam.{fmt}",
    response_model=IDOTRWIScamSchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
def idot_rwiscam_service(
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


idot_rwiscam_service.__doc__ = __doc__
