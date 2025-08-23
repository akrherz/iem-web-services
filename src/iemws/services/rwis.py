"""IEM Roadway Weather Information Service (RWIS)

This API emits all RWIS observation data collected by the IEM at a given
valid timestamp.  The archive is searched back 60 minutes prior to the given
timestamp for the most recent observation.  Most of the RWIS data arrives to
the IEM via MADIS, which has some delays with processing.  The data here is
not necessarily super timely.
"""

from datetime import datetime, timedelta, timezone

import geopandas as gpd
import numpy as np
from fastapi import APIRouter, Query
from pyiem.database import sql_helper
from pyiem.util import utc

from ..models import SupportedFormats
from ..models.rwis import RWISSchema
from ..util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()

# Database sync happens frequently enough that we can just use the archive
SQL = """
with agg as (
    SELECT c.iemid, rank() OVER (PARTITION by c.iemid ORDER by c.valid DESC),
    t.tzname, t.id as station,
    t.name, t.county, t.state, t.network,
    to_char(c.valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    to_char(c.valid at time zone t.tzname,
            'YYYY-MM-DDThh24:MI:SS') as local_valid,
    tmpf, dwpf, relh, vsby, sknt, drct,
    subf, gust, tfs0_text, tfs1_text, tfs2_text, tfs3_text,
    tfs0, tfs1, tfs2, tfs3,
    pcpn, feel, t.geom, ST_x(t.geom) as lon, ST_y(t.geom) as lat
    from alldata c JOIN stations t on (c.iemid = t.iemid) WHERE
    c.valid > :sts and c.valid <= :ets)
select * from agg where rank = 1
"""


def compute(df):
    """Compute other things that we can't easily do in the database"""
    # replace any None values with np.nan
    return df.fillna(value=np.nan)


def handler(valid: datetime):
    """Handle the request, return dict"""
    params = {"sts": valid - timedelta(minutes=60), "ets": valid}
    with get_sqlalchemy_conn("rwis") as conn:
        df = gpd.read_postgis(
            sql_helper(SQL),
            conn,
            params=params,
            index_col="station",
            geom_col="geom",
        )  # type: ignore
    return compute(df)


@router.get(
    "/rwis.{fmt}",
    response_model=RWISSchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(120)
def rwis_service(
    fmt: SupportedFormats,
    valid: datetime = Query(None, description="UTC Timestamp"),
):
    """Replaced above with module __doc__"""
    if valid is None:
        valid = utc()
    elif valid.tzinfo is None:
        valid = valid.replace(tzinfo=timezone.utc)

    return deliver_df(handler(valid), fmt)


# Not really used
rwis_service.__doc__ = __doc__
