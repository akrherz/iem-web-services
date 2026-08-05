"""# Backend for FACTS Drydown tool.

The drydown tool website [FACTS](https://facts.extension.iastate.edu/corn-drydown-calculator).

## Changelog

- 5 August 2026: The CFS forecast is no longer included and the service now
  allows for specification of inclusive `sday` and `eday` values.

"""

from typing import Annotated

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from metpy.units import units
from pyiem.database import sql_helper
from pyiem.iemre import get_domain, get_gid
from pyiem.util import logger

from ..util import get_sqlalchemy_conn

LOG = logger()
router = APIRouter()


def _i(val):
    """Safe conversion to int."""
    if np.ma.is_masked(val):
        return None
    return int(val)


def handler(lon: float, lat: float, sday: str, eday: str):
    """Handle the request."""
    domain = get_domain(lon, lat)
    if domain is None:
        raise HTTPException(status_code=404, detail="Point outside of IEMRE")
    gid = get_gid(lon, lat, domain)
    dbname = f"iemre_{domain}" if domain != "conus" else "iemre"

    with get_sqlalchemy_conn(dbname) as conn:
        df = pd.read_sql(
            sql_helper("""
            SELECT valid, high_tmpk, low_tmpk, (max_rh + min_rh) / 2 as avg_rh
            from iemre_daily WHERE gid = :gid and valid > '1980-01-01' and
            to_char(valid, 'mmdd') between :sday and :eday
            and high_tmpk is not null and low_tmpk is not null
            ORDER by valid ASC
        """),
            conn,
            params={"gid": gid, "sday": sday, "eday": eday},
            parse_dates=["valid"],
            index_col=None,
        )
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found.")
    df["max_tmpf"] = (df["high_tmpk"].to_numpy() * units.degK).to(units.degF).m
    df["min_tmpf"] = (df["low_tmpk"].to_numpy() * units.degK).to(units.degF).m

    df["year"] = df["valid"].dt.year
    res = {"data": {}}
    for year, df2 in df.groupby("year"):
        res["data"][year] = {
            "dates": df2["valid"].dt.strftime("%Y-%m-%d").values.tolist(),
            "high": df2["max_tmpf"].values.astype("i").tolist(),
            "low": df2["min_tmpf"].values.astype("i").tolist(),
            "rh": df2["avg_rh"].values.astype("i").tolist(),
        }
    return res


@router.get(
    "/drydown.json",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def drydown_service(
    lat: Annotated[float, Query(description="North Latitude")],
    lon: Annotated[float, Query(description="East Longitude")],
    sday: Annotated[
        str, Query(description="Inclusive start date mmdd", pattern=r"^\d{4}$")
    ] = "0901",
    eday: Annotated[
        str, Query(description="Inclusive end date mmdd", pattern=r"^\d{4}$")
    ] = "1130",
):
    """Docscript replaced above."""
    if sday > eday:
        raise HTTPException(
            status_code=400, detail="Start date must be before end date"
        )
    return handler(lon, lat, sday, eday)
