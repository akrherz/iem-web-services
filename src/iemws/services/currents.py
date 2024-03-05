"""IEM Currents Service.

You can approach this API via the following ways:
 - `/currents.json?network=IA_ASOS` :: A single "network" worth of currents.
 - `/currents.json?networkclass=COOP&wfo=DMX` :: All COOP sites for WFO DMX
 - `/currents.json?networkclass=ASOS&country=US` :: All ASOS sites for US
 - `/currents.json?state=IA` :: Everything the IEM has for Iowa
 - `/currents.json?wfo=DMX` :: Everything the IEM has for WFO DMX
 - `/currents.json?station=DSM&station=AMW` :: Explicit listing of stations
 - `/currents.json?event=ice_accretion_1hr` :: Special METAR service.
 - `/currents.json?network=CCOOP` :: Special for CCOOP sites.

For better or worse, the ".json" in the URI path above controls the output
format that the service emits.  This service supports ".json", ".geojson",
and ".txt" (comma delimited) formats.
"""

from datetime import date, timedelta
from typing import List

import geopandas as gpd
import numpy as np
from fastapi import APIRouter, Query
from sqlalchemy import text

from ..models import SupportedFormats
from ..models.currents import CurrentsSchema
from ..util import cache_control, deliver_df, get_sqlalchemy_conn

router = APIRouter()

# Avoid three table aggregate by initial window join
SQL = """
WITH agg as (
    SELECT c.iemid, t.tzname, t.id, c.valid,
    t.id as station, t.name, t.county, t.state, t.network,
    to_char(c.valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    to_char(c.valid at time zone t.tzname,
            'YYYY-MM-DDThh24:MI:SS') as local_valid,
    tmpf, dwpf, relh, vsby, sknt, drct,
    c1smv, c2smv, c3smv, c4smv, c5smv,
    c1tmpf, c2tmpf, c3tmpf, c4tmpf, c5tmpf,
    c.pday as ob_pday, c.pmonth as ob_pmonth,
    gust, mslp, pres,
    scond0, scond1, scond2, scond3, srad,
    tsf0, tsf1, tsf2, tsf3, rwis_subf, raw, phour, feel,
    ice_accretion_1hr, ice_accretion_3hr, ice_accretion_6hr,
    skyl1, skyc1, skyl2, skyc2, skyl3, skyc3, skyl4, skyc4, alti,
    array_to_string(wxcodes, ' ') as wxcodes,
    t.geom, ST_x(t.geom) as lon, ST_y(t.geom) as lat
    from current c JOIN stations t on (c.iemid = t.iemid) WHERE
    REPLACEME not t.metasite and t.online
    and c.valid > (now() - :dtinterval)
)
    SELECT c.id as station, c.name, c.county, c.state, c.network,
    to_char(s.day, 'YYYY-mm-dd') as local_date, snow, snowd, snoww,
    c.utc_valid, c.local_valid,
    tmpf, max_tmpf, min_tmpf, dwpf, relh, vsby, sknt, drct,
    c1smv, c2smv, c3smv, c4smv, c5smv,
    c1tmpf, c2tmpf, c3tmpf, c4tmpf, c5tmpf,
    ob_pday, ob_pmonth, s.pmonth as s_pmonth,
    max_sknt, max_gust, gust, mslp, pres,
    scond0, scond1, scond2, scond3, srad,
    tsf0, tsf1, tsf2, tsf3, rwis_subf, raw, phour, feel,
    ice_accretion_1hr, ice_accretion_3hr, ice_accretion_6hr,
    skyl1, skyc1, skyl2, skyc2, skyl3, skyc3, skyl4, skyc4, alti,
    wxcodes,
    geom,
    to_char(s.max_gust_ts at time zone 'UTC',
        'YYYY-MM-DDThh24:MI:SSZ') as utc_max_gust_ts,
    to_char(s.max_gust_ts at time zone c.tzname,
            'YYYY-MM-DDThh24:MI:SS') as local_max_gust_ts,
    to_char(s.max_sknt_ts at time zone 'UTC',
        'YYYY-MM-DDThh24:MI:SSZ') as utc_max_sknt_ts,
    to_char(s.max_sknt_ts at time zone c.tzname,
            'YYYY-MM-DDThh24:MI:SS') as local_max_sknt_ts,
    lon, lat, s.pday
    from agg c JOIN summary s on
    (c.iemid = s.iemid and s.day = date(c.valid at time zone c.tzname))
"""


def compute(df):
    """Compute other things that we can't easily do in the database"""
    # replace any None values with np.nan
    return df.fillna(value=np.nan)
    # contraversy here, drop any columns that are all missing
    # df.dropna(how='all', axis=1, inplace=True)


def handler(
    network,
    networkclass,
    wfo,
    country,
    state,
    station,
    event,
    minutes,
):
    """Handle the request, return dict"""
    params = {}
    if network == "CCOOP":
        sql = SQL.replace(
            "REPLACEME", "t.network ~* 'DCP' and t.name ~* 'CCOOP' and"
        )
    elif station is not None:
        sql = SQL.replace("REPLACEME", "t.id = ANY(:ids) and")
        params["ids"] = list(station)
    elif networkclass is not None and wfo is not None:
        sql = SQL.replace(
            "REPLACEME", "t.wfo = :wfo and t.network ~* :network and"
        )
        params["wfo"] = wfo
        params["network"] = networkclass
    elif networkclass is not None and country is not None:
        sql = SQL.replace(
            "REPLACEME", "t.country = :country and t.network ~* :network and"
        )
        params["country"] = country
        params["network"] = networkclass
    elif wfo is not None:
        sql = SQL.replace("REPLACEME", "t.wfo = :wfo and")
        params["wfo"] = wfo
    elif state is not None:
        sql = SQL.replace("REPLACEME", "t.state = :state and")
        params["state"] = state
    elif network is not None:
        sql = SQL.replace("REPLACEME", "t.network = :network and")
        params["network"] = network
    else:
        # This is expensive, throttle things back some
        sql = SQL.replace("REPLACEME", "").replace(
            " summary ", f" summary_{date.today().year} "
        )
        minutes = min([minutes, 600])

    params["dtinterval"] = timedelta(minutes=minutes)
    with get_sqlalchemy_conn("iem") as conn:
        df = gpd.read_postgis(
            text(sql),
            conn,
            params=params,
            index_col="station",
            geom_col="geom",
        )
    if event is not None and event in df.columns:
        df = df[df[event].notna()]
    df = compute(df)
    return df


@router.get(
    "/currents.{fmt}",
    response_model=CurrentsSchema,
    description=__doc__,
    tags=[
        "iem",
    ],
)
@cache_control(120)
def currents_service(
    fmt: SupportedFormats,
    network: str = Query(None, description="IEM Network Identifier"),
    networkclass: str = Query(None),
    wfo: str = Query(None, max_length=4),
    country: str = Query(None, max_length=2),
    state: str = Query(None, max_length=2),
    station: List[str] = Query(None),
    event: str = Query(None),
    minutes: int = Query(1440 * 10),
):
    """Replaced above with module __doc__"""

    df = handler(
        network,
        networkclass,
        wfo,
        country,
        state,
        station,
        event,
        minutes,
    )
    return deliver_df(df, fmt)


# Not really used
currents_service.__doc__ = __doc__
