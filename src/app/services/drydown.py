"""Provide data to support drydown app."""
import warnings
import datetime
import os
import json
from io import StringIO

import numpy as np
from metpy.units import units
from metpy.calc import relative_humidity_from_dewpoint
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, ncopen
from pyiem.iemre import get_gid, find_ij, daily_offset

# prevent warnings that may trip up mod_wsgi
warnings.simplefilter("ignore")

CACHE_EXPIRE = 3600
# Avoid three table aggregate by initial window join


def get_mckey(fields):
    """What's the key for this request"""
    return "%s_%s" % (fields.get("lat", ""), fields.get("lon", ""))


def append_cfs(res, lon, lat):
    """Append on needed CFS data."""
    gridx, gridy = find_ij(lon, lat)
    lastyear = max(res["data"].keys())
    thisyear = datetime.date.today().year
    if lastyear != thisyear:
        # We don't have any data yet for this year, so we add some
        res["data"][thisyear] = {"dates": [], "high": [], "low": [], "rh": []}
        lastdate = datetime.date(thisyear, 8, 31)
    else:
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
            return
    nc = ncopen(testfn)
    high = (nc.variables["high_tmpk"][:, gridy, gridx] * units.degK).to(units.degF).m
    low = (nc.variables["low_tmpk"][:, gridy, gridx] * units.degK).to(units.degF).m
    # RH hack
    # found ~20% bias with this value, so arb addition for now
    rh = (
        relative_humidity_from_dewpoint(high * units.degF, low * units.degF).m * 100.0
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
            entry["high"].append(int(high[i]))
            entry["low"].append(int(low[i]))
            entry["rh"].append(int(rh[i]))


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
