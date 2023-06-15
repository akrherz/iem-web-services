"""ISU Soil Moisture Network (Multi)-Daily Data.

Returns Iowa State University Soil Moisture Network daily or multi daily
summary information.  Please note that the end date is inclusive.
"""
from datetime import date, datetime

import numpy as np
from fastapi import APIRouter, Query
from geopandas import read_postgis
from pandas.io.sql import read_sql
from pyiem.tracker import loadqc
from sqlalchemy import text

# Local
from ...models import SupportedFormats
from ...util import deliver_df, get_dbconn

router = APIRouter()


def iemtracker(df, edate):
    """Figure out what should be QC'd out."""
    qcdict = loadqc(date=edate)
    for idx, row in df.iterrows():
        qc = qcdict.get(row["station"], {})
        if qc.get("precip"):
            df.at[idx, "precip"] = np.nan
        if qc.get("soil4"):
            df.at[idx, "sgdd"] = np.nan
        if qc.get("tmpf"):
            df.at[idx, "gdd"] = np.nan

    return df


def get_climo(sdate, edate, gddbase, gddceil):
    """Build the climatology dataframe."""
    df = read_sql(
        """
        with climo as (
            select station, sday,
            avg(gddxx(%s, %s, high, low)) as gdd,
            avg(precip) as precip
            from alldata_ia WHERE
            station in (select distinct climate_site from stations where
                network in ('ISUAG', 'ISUSM')) and
            sday >= %s and sday <= %s
            GROUP by station, sday
        ), agg as (
            select station, sum(gdd) as climo_gdd, sum(precip) as climo_precip
            from climo GROUP by station
        )
        select id as station, climo_gdd, climo_precip from agg a, stations t
        WHERE t.network in ('ISUAG', 'ISUSM') and t.climate_site = a.station
        """,
        get_dbconn("coop"),
        params=(
            gddbase,
            gddceil,
            sdate.strftime("%m%d"),
            edate.strftime("%m%d"),
        ),
    )
    return df


def get_df_isuag(sdate, edate, gddbase, gddceil):
    """Here we go."""
    pgconn = get_dbconn("isuag")
    ets = datetime(edate.year, edate.month, edate.day, 23, 59)
    df = read_postgis(
        """
        WITH hourly_daily as (
            SELECT station, date(valid),
            max(c300) as tsoil_high,
            min(c300) as tsoil_low
            from hourly WHERE valid >= %s and valid < %s
            GROUP by station, date
        ), hourly_agg as (
            select station,
            sum(gddxx(%s, %s, tsoil_high, tsoil_low)) as sgdd
            from hourly_daily GROUP by station
        ), daily_agg as (
            select station,
            sum(c70) as et,
            sum(gddxx(%s, %s, c11, c12)) as gdd,
            sum(c80) as srad,
            sum(c90) as precip
            from daily WHERE valid >= %s and valid <= %s
            GROUP by station
        ), agg as (
            select d.*, h.sgdd from
            daily_agg d JOIN hourly_agg h on (d.station = h.station)
        )
        select a.*, st_x(geom) as lon, st_y(geom) as lat, geom, name,
        plot_name as city from
        agg a JOIN stations t on (a.station = t.id) WHERE t.network = 'ISUAG'
        """,
        pgconn,
        params=(sdate, ets, gddbase, gddceil, gddbase, gddceil, sdate, edate),
        index_col=None,
        geom_col="geom",
    )
    return df


def get_df_isusm(sdate, edate, gddbase, gddceil):
    """Here we go."""
    pgconn = get_dbconn("isuag")
    ets = datetime(edate.year, edate.month, edate.day, 23, 59)
    params = {
        "gddbase": gddbase,
        "gddceil": gddceil,
        "sdate": sdate,
        "ets": ets,
    }
    df = read_postgis(
        text(
            """
        WITH daily_agg as (
            select station,
            sum(dailyet_qc / 25.4) as et,
            sum(gddxx(
                :gddbase, :gddceil, c2f(t4_c_max_qc), c2f(t4_c_min_qc)))
                as sgdd,
            sum(gddxx(
                :gddbase, :gddceil, c2f(tair_c_max_qc), c2f(tair_c_min_qc)))
                as gdd,
            sum(slrkj_tot_qc) / 1000. as srad,
            sum(rain_in_tot_qc) as precip
            from sm_daily WHERE valid >= :sdate and valid <= :ets
            GROUP by station
        )
        select d.*, st_x(geom) as lon, st_y(geom) as lat, geom, name,
        plot_name as city from
        daily_agg d JOIN stations t on (d.station = t.id)
        WHERE t.network = 'ISUSM'
        """
        ),
        pgconn,
        params=params,
        index_col=None,
        geom_col="geom",
    )
    return df


def handler(sdate, edate, gddbase, gddceil):
    """Handle the request, return dict"""
    if edate is None:
        edate = sdate

    climo = get_climo(sdate, edate, gddbase, gddceil)
    if sdate.year > 2013:
        df = get_df_isusm(sdate, edate, gddbase, gddceil)
    else:
        df = get_df_isuag(sdate, edate, gddbase, gddceil)
    df = df.merge(climo, how="left", on="station")
    df = iemtracker(df, edate)
    return df


@router.get(
    "/isusm/daily.{fmt}",
    description=__doc__,
    tags=[
        "iem",
    ],
)
def service(
    fmt: SupportedFormats,
    sdate: date = Query(...),
    edate: date = Query(None),
    gddbase: int = Query(50),
    gddceil: int = Query(86),
):
    """Replaced above."""
    df = handler(sdate, edate, gddbase, gddceil)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
