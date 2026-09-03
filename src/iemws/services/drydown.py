"""# Backend for FACTS Drydown tool.

The drydown tool website
[FACTS](https://facts.extension.iastate.edu/corn-drydown-calculator).

## Changelog

- 2 September 2026: The CFS data was replaced with the control and 30 member
  GEFS ensemble.
- 8 August 2026: This service now emits a CFS forecast for all IEMRE domains.
- 7 August 2026: The CFS forecast is back, but the data is included in the
  root `forecast` key within the JSON response. It also ignores whatever
  `sday` and `eday` parameters are set.
- 5 August 2026: The CFS forecast is no longer included and the service now
  allows for specification of inclusive `sday` and `eday` values.

"""

from datetime import datetime, timezone
from typing import Annotated

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from metpy.units import units
from pyiem.database import sql_helper
from pyiem.iemre import get_domain, get_gid
from pyiem.util import get_properties, logger

from ..util import get_sqlalchemy_conn

LOG = logger()
router = APIRouter()


def _i(val):
    """Safe conversion to int."""
    if np.ma.is_masked(val):
        return None
    return int(val)


def append_gefs(lon: float, lat: float, res: dict) -> None:
    """Handle the request for the latest GEFS forecast."""
    # This can't fail as it didn't fail in handler
    domain = get_domain(lon, lat)
    gid = get_gid(lon, lat, domain=domain)
    gefs_valid = datetime.strptime(
        get_properties().get(f"iemre.gefs.{domain}", "2026-05-01T12:00Z"),
        "%Y-%m-%dT%H:%MZ",
    ).replace(tzinfo=timezone.utc)
    with get_sqlalchemy_conn(
        "iemre" if domain == "conus" else f"iemre_{domain}"
    ) as conn:
        fxdf = pd.read_sql(
            sql_helper("""
            select ens_member, valid, high_tmpk, low_tmpk, avg_rh
            from iemre_gefs where model_valid = :mv and gid = :gid
            order by ens_member asc, valid asc
            """),
            conn,
            params={"mv": gefs_valid, "gid": gid},
            parse_dates=["valid"],
            index_col=None,
        )
    if fxdf.empty:
        return
    fxdf["high"] = (fxdf["high_tmpk"].to_numpy() * units.degK).to(units.degF).m
    fxdf["low"] = (fxdf["low_tmpk"].to_numpy() * units.degK).to(units.degF).m
    for ens in range(31):
        df2 = fxdf[fxdf["ens_member"] == ens]
        if df2.empty:
            continue
        if ens == 0:
            res["forecast"]["dates"] = (
                df2["valid"].dt.strftime("%Y-%m-%d").values.tolist()
            )
        # list of lists
        res["forecast"]["high"].append(df2["high"].values.astype("i").tolist())
        res["forecast"]["low"].append(df2["low"].values.astype("i").tolist())
        res["forecast"]["rh"].append(df2["avg_rh"].values.astype("i").tolist())


def handler(lon: float, lat: float, sday: str, eday: str) -> dict:
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
    res = {
        "data": {},
        "forecast": {"dates": [], "high": [], "low": [], "rh": []},
    }
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
    res = handler(lon, lat, sday, eday)
    append_gefs(lon, lat, res)
    return res
