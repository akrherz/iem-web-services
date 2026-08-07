"""# Backend for FACTS Drydown tool.

The drydown tool website
[FACTS](https://facts.extension.iastate.edu/corn-drydown-calculator).

## Changelog

- 7 August 2026: The CFS forecast is back, but the data is included in the
  root `forecast` key within the JSON response. It also ignores whatever
  `sday` and `eday` parameters are set.
- 5 August 2026: The CFS forecast is no longer included and the service now
  allows for specification of inclusive `sday` and `eday` values.

"""

from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from metpy.calc import relative_humidity_from_dewpoint
from metpy.units import masked_array, units
from pyiem.database import sql_helper
from pyiem.iemre import find_ij, get_domain, get_gid
from pyiem.util import logger, ncopen

from ..util import get_sqlalchemy_conn

LOG = logger()
NCOPEN_TIMEOUT = 30
router = APIRouter()


def _i(val):
    """Safe conversion to int."""
    if np.ma.is_masked(val):
        return None
    return int(val)


def append_cfs(lon: float, lat: float, res: dict) -> None:
    """Handle the request for the latest CFS forecast."""
    gridx, gridy = find_ij(lon, lat)
    # go find the most recent CFS 0z file
    for offset in range(2, 11):
        valid = date.today() - timedelta(days=offset)
        testfn = valid.strftime("/mesonet/data/iemre/cfs_%Y%m%d00.nc")
        if Path(testfn).is_file():
            break
        if offset == 10:
            LOG.info("No CFS file found for %s", valid)
            return
    try:
        nc = ncopen(testfn, timeout=NCOPEN_TIMEOUT)
    except Exception as exp:
        LOG.error(exp)
        return
    if nc is None:
        LOG.debug("Failing %s as nc is None", testfn)
        return
    high = (
        masked_array(nc.variables["high_tmpk"][:, gridy, gridx], units.degK)
        .to(units.degF)
        .m
    )
    low = (
        masked_array(nc.variables["low_tmpk"][:, gridy, gridx], units.degK)
        .to(units.degF)
        .m
    )
    # RH hack
    # found ~20% bias with this value, so arb addition for now
    rh = (
        relative_humidity_from_dewpoint(
            masked_array(high, units.degF), masked_array(low, units.degF)
        ).m
        * 100.0
        + 20.0
    )
    rh = np.where(rh > 95, 95, rh)
    times = nc.variables["time"][:]  # days since the start of this year
    nc.close()
    skip_first_row = True
    for i, tidx in enumerate(times):
        hval = _i(high[i])
        if hval is not None:
            if skip_first_row:
                skip_first_row = False
                continue
            lts = date(valid.year, 1, 1) + timedelta(days=tidx)
            res["forecast"]["dates"].append(lts.strftime("%Y-%m-%d"))
            res["forecast"]["high"].append(hval)
            res["forecast"]["low"].append(_i(low[i]))
            res["forecast"]["rh"].append(_i(rh[i]))


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
    append_cfs(lon, lat, res)
    return res
