"""Exposes Iowa DOT 'Dashcam' imagery from its snowplows."""
from datetime import datetime, timedelta, timezone
import tempfile

# third party
from pyiem.util import utc
import pandas as pd
from geopandas import read_postgis
from fastapi import Response, Query

# Local
from ..util import get_dbconn
from ..models import SupportedFormats
from ..models.idot_dashcam import RootSchema
from ..reference import MEDIATYPES


def make_url(row):
    """Convert into web address."""
    return row["utc_valid"].strftime(
        "https://mesonet.agron.iastate.edu/archive/data/%Y/%m/%d/"
        f"camera/idot_trucks/{row['cid']}/{row['cid']}_%Y%m%d%H%M.jpg"
    )


def handler(valid, window, fmt):
    """Do the requested work."""
    pgconn = get_dbconn("postgis")
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
    if fmt != "geojson":
        df = df.drop("geom", axis=1)
        df = pd.DataFrame(df)
    if fmt == "txt":
        return df.to_csv(index=False)
    if fmt == "json":
        # Implement our 'table-schema' option
        return df[["utc_valid"]].to_json(orient="table", index=False)
    if df.empty:
        return '{"type": "FeatureCollection", "features": []}'
    with tempfile.NamedTemporaryFile("w", delete=True) as tmp:
        df.to_file(tmp.name, driver="GeoJSON")
        with open(tmp.name) as fh:
            res = fh.read()
    return res


def factory(app):
    """Generate."""

    @app.get(
        "/idot_dashcam.{fmt}", response_model=RootSchema, description=__doc__
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
            valid = utc() - timedelta(minutes=window)
        if valid.tzinfo is None:
            valid = valid.replace(tzinfo=timezone.utc)
        return Response(
            handler(valid, window, fmt), media_type=MEDIATYPES[fmt]
        )

    idot_dashcam_service.__doc__ = __doc__
