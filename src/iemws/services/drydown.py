"""Provide data to support drydown app."""
import datetime
import os

import numpy as np
from metpy.units import units, masked_array
from metpy.calc import relative_humidity_from_dewpoint
from pandas.io.sql import read_sql
from fastapi import Query, HTTPException, APIRouter
from pyiem.util import ncopen, logger
from pyiem.iemre import get_gid, find_ij, daily_offset
from ..util import get_dbconn

LOG = logger()
NCOPEN_TIMEOUT = 20  # seconds
router = APIRouter()


def _i(val):
    """Safe conversion to int."""
    if np.ma.is_masked(val):
        return None
    return int(val)


def append_cfs(res, lon, lat):
    """Append on needed CFS data."""
    gridx, gridy = find_ij(lon, lat)
    lastyear = max(res["data"].keys())
    thisyear = datetime.date.today().year
    lastdate = datetime.date(thisyear, 8, 31)
    if lastyear != thisyear:
        # We don't have any data yet for this year, so we add some
        res["data"][thisyear] = {"dates": [], "high": [], "low": [], "rh": []}
    else:
        # shrug
        if res["data"][lastyear]["dates"]:
            lastdate = datetime.datetime.strptime(
                res["data"][thisyear]["dates"][-1], "%Y-%m-%d"
            ).date()
    # go find the most recent CFS 0z file
    valid = datetime.date.today()
    attempt = 0
    while True:
        testfn = valid.strftime("/mesonet/data/iemre/cfs_%Y%m%d00.nc")
        if os.path.isfile(testfn):
            break
        valid -= datetime.timedelta(hours=24)
        attempt += 1
        if attempt > 9:
            return None
    try:
        nc = ncopen(testfn, timeout=NCOPEN_TIMEOUT)
    except Exception as exp:
        LOG.error(exp)
        return None
    if nc is None:
        LOG.debug("Failing %s as nc is None", testfn)
        return None
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
    entry = res["data"][thisyear]
    # lastdate is either August 31 or a date after, so our first forecast
    # date is i+1
    tidx = daily_offset(lastdate + datetime.timedelta(days=1))
    for i in range(tidx, 365):
        lts = datetime.date(thisyear, 1, 1) + datetime.timedelta(days=i)
        if lts.month in [9, 10, 11]:
            entry["dates"].append(lts.strftime("%Y-%m-%d"))
            entry["high"].append(_i(high[i]))
            entry["low"].append(_i(low[i]))
            entry["rh"].append(_i(rh[i]))
    return res


def handler(lon, lat):
    """Handle the request."""
    gid = get_gid(lon, lat)

    pgconn = get_dbconn("iemre")
    df = read_sql(
        """
        SELECT valid, high_tmpk, low_tmpk, (max_rh + min_rh) / 2 as avg_rh
        from iemre_daily WHERE gid = %s and valid > '1980-01-01' and
        to_char(valid, 'mmdd') between '0901' and '1201'
        and high_tmpk is not null and low_tmpk is not null
        ORDER by valid ASC
    """,
        pgconn,
        params=(int(gid),),
        parse_dates="valid",
        index_col=None,
    )
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found.")
    df["max_tmpf"] = (df["high_tmpk"].values * units.degK).to(units.degF).m
    df["min_tmpf"] = (df["low_tmpk"].values * units.degK).to(units.degF).m
    df["avg_rh"] = df["avg_rh"].fillna(50)

    df["year"] = df["valid"].dt.year
    res = {"data": {}}
    for year, df2 in df.groupby("year"):
        res["data"][year] = {
            "dates": df2["valid"].dt.strftime("%Y-%m-%d").values.tolist(),
            "high": df2["max_tmpf"].values.astype("i").tolist(),
            "low": df2["min_tmpf"].values.astype("i").tolist(),
            "rh": df2["avg_rh"].values.astype("i").tolist(),
        }
    append_cfs(res, lon, lat)
    return res


@router.get("/drydown.json")
def drydown_service(lat: float = Query(...), lon: float = Query(...)):
    """Babysteps."""
    return handler(lon, lat)


drydown_service.__doc__ = __doc__
