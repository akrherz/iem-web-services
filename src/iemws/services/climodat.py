"""Climodat services."""

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem.database import sql_helper

from ..models import SupportedFormatsNoGeoJSON
from ..models.climodat import PORDailyClimoSchema
from ..util import deliver_df, get_sqlalchemy_conn

router = APIRouter()


def handler(station):
    """Handle the request."""

    # Load up the data and let pandas do the heavy lifting
    with get_sqlalchemy_conn("coop") as conn:
        obs = pd.read_sql(
            sql_helper(
                "select sday, year, high, low, precip from alldata "
                "WHERE station = :station ORDER by day asc"
            ),
            conn,
            params={"station": station},
            index_col=None,
        )
    if obs.empty:
        raise HTTPException(404, "No data found for query.")
    sdays = pd.date_range("2000/1/1", "2000/12/31").strftime("%m%d")
    stats = (
        obs[["sday", "high", "low", "precip"]]
        .groupby("sday")
        .agg(["mean", "min", "max", "count"])
        .reindex(sdays)
    )
    max_precip_years = (
        obs[obs.groupby("sday")["precip"].transform("max") == obs["precip"]]
        .groupby("sday")["year"]
        .apply(lambda x: " ".join(map(str, x)))
        .reindex(sdays)
    )
    max_high_years = (
        obs[obs.groupby("sday")["high"].transform("max") == obs["high"]]
        .groupby("sday")["year"]
        .apply(lambda x: " ".join(map(str, x)))
        .reindex(sdays)
    )
    min_high_years = (
        obs[obs.groupby("sday")["high"].transform("min") == obs["high"]]
        .groupby("sday")["year"]
        .apply(lambda x: " ".join(map(str, x)))
        .reindex(sdays)
    )
    max_low_years = (
        obs[obs.groupby("sday")["low"].transform("max") == obs["low"]]
        .groupby("sday")["year"]
        .apply(lambda x: " ".join(map(str, x)))
        .reindex(sdays)
    )
    min_low_years = (
        obs[obs.groupby("sday")["low"].transform("min") == obs["low"]]
        .groupby("sday")["year"]
        .apply(lambda x: " ".join(map(str, x)))
        .reindex(sdays)
    )
    df = pd.DataFrame(
        {
            "station": station,
            "date": pd.date_range("2000/1/1", "2000/12/31").strftime(
                "%Y-%m-%d"
            ),
            "years": stats[("high", "count")].values,
            "high_min": stats[("high", "min")].values,
            "high_min_years": min_high_years.values,
            "high_avg": stats[("high", "mean")].values,
            "high_max": stats[("high", "max")].values,
            "high_max_years": max_high_years.values,
            "low_min": stats[("low", "min")].values,
            "low_min_years": min_low_years.values,
            "low_avg": stats[("low", "mean")].values,
            "low_max": stats[("low", "max")].values,
            "low_max_years": max_low_years.values,
            "precip": stats[("precip", "mean")].values,
            "precip_max": stats[("precip", "max")].values,
            "precip_max_years": max_precip_years.values,
        }
    ).round(2)
    return df


@router.get(
    "/climodat/por_daily_climo.{fmt}",
    description="""
IEM Climodat Period of Record (POR) Daily Climatology

This service provides a daily summary of climate variables for a given
IEM Climodat station.  The year 2000 is chosen for the daily climatology
dates.
    """,
    tags=[
        "iem",
    ],
    response_model=PORDailyClimoSchema,
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    station: str = Query(
        ..., description="IEM Climodat Station Identifier", max_length=6
    ),
):
    """."""
    return deliver_df(handler(station), fmt)
