"""IEM Station Metadata for One Indentifier.

The IEM uses standardized station identifiers whenever possible.  This service
returns metadata for a given station idenitifier. Note that some identifiers
are shared between multiple IEM network labels, so you will get multiple
results in some cases.
"""

import geopandas as gpd
from fastapi import APIRouter, HTTPException, Path
from pyiem.database import sql_helper

# Local
from ..models import SupportedFormats
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(station_id):
    """Handle the request, return dict"""
    with get_sqlalchemy_conn("mesosite") as pgconn:
        df = gpd.read_postgis(
            sql_helper(
                "SELECT *, ST_X(geom) as longitude, ST_Y(geom) as latitude "
                "from stations where id = :station_id ORDER by name ASC"
            ),
            pgconn,
            params={"station_id": station_id},
            geom_col="geom",
            index_col=None,
        )  # type: ignore
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="No stations found.",
        )
    # Hackaround downstream issues
    df["archive_begin"] = df["archive_begin"].astype("datetime64[ns]")
    df["archive_end"] = df["archive_end"].astype("datetime64[ns]")
    return df


@router.get(
    "/station/{station_id}.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormats,
    station_id: str = Path(..., description="IEM Station Identifier."),
):
    """Replaced above."""
    df = handler(station_id)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
