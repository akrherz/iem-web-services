"""Exposes Iowa DOT Winter Road Conditions."""
from datetime import datetime, timedelta, timezone
import tempfile

# third party
from pyiem.util import utc
import pandas as pd
from geopandas import read_postgis
from fastapi import Response, Query, APIRouter

# Local
from ..util import get_dbconn
from ..models import SupportedFormats
from ..reference import MEDIATYPES

router = APIRouter()


def handler(valid, fmt):
    """Do the requested work."""
    pgconn = get_dbconn("postgis")
    df = read_postgis(
        """
        WITH data as (
            select b.type as rtype, b.int1, b.segid, c.cond_code, c.valid,
            row_number() OVER (PARTITION by b.segid ORDER by c.valid DESC)
            from roads_base b, roads_log c WHERE b.segid = c.segid
            and c.valid > %s and c.valid < %s),
        agg as (
            select * from data where row_number = 1),
        agg2 as (
            select valid at time zone 'UTC' as utc_valid,
            b.type as rtype, b.int1, b.st1, b.us1,
            ST_Transform(b.geom, 4326) as geom,
            coalesce(d.cond_code, 0) as cond_code from
            roads_base b JOIN agg d on (b.segid = d.segid))
        select a2.*, c.color, c.label from agg2 a2 JOIN roads_conditions c
        on (a2.cond_code = c.code)
        """,
        pgconn,
        params=(valid - timedelta(days=30), valid),
        geom_col="geom",
        index_col=None,
    )
    if not df.empty:
        df["utc_valid"] = df["utc_valid"].dt.strftime("%Y-%m-%dT%H:%MZ")
    if fmt != "geojson":
        df = df.drop("geom", axis=1)
        df = pd.DataFrame(df)
    if fmt == "txt":
        return df.to_csv(index=False)
    if fmt == "json":
        # Implement our 'table-schema' option
        return df.to_json(orient="table", index=False)
    if df.empty:
        return '{"type": "FeatureCollection", "features": []}'
    with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
        df.to_file(tmp.name, driver="GeoJSON")
        with open(tmp.name) as fh:
            res = fh.read()
    return res


@router.get("/iowa_winter_roadcond.{fmt}", description=__doc__)
def service(
    fmt: SupportedFormats,
    valid: datetime = Query(
        None, description="UTC timestamp to look for conditions."
    ),
):
    """Replaced Below."""
    if valid is None:
        valid = utc()
    if valid.tzinfo is None:
        valid = valid.replace(tzinfo=timezone.utc)
    return Response(handler(valid, fmt), media_type=MEDIATYPES[fmt])


service.__doc__ = __doc__
