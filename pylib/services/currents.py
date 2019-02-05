"""Currents API endpoint supporting a number of calls

    /api/1/currents.txt?network=IA_ASOS
    /api/1/currents.txt?networkclass=COOP&wfo=DMX
    /api/1/currents.txt?state=IA
    /api/1/currents.txt?wfo=DMX
    /api/1/currents.txt?station=DSM&station=AMW
    /api/1/currents.txt?event=ice_accretion_1hr
"""
import os
import tempfile
import warnings

import numpy as np
from pandas.io.sql import read_sql
from geopandas import read_postgis
from pyiem.util import get_dbconn
# prevent warnings that may trip up mod_wsgi
warnings.simplefilter('ignore')

CACHE_EXPIRE = 60
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
    REPLACEME not t.metasite
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
    from agg c, summary s WHERE s.day >= 'TODAY':: date - '2 days'::interval
    and c.iemid = s.iemid and s.day = date(c.valid at time zone c.tzname)
"""


def get_mckey(fields):
    """What's the key for this request"""
    return "%s_%s_%s_%s_%s_%s" % (
        fields.get('network', ''), fields.get('networkclass', ''),
        fields.get('wfo', ''), fields.get('state', ''),
        ",".join(fields.getall('station')), fields.get('event', '')
    )


def compute(df):
    """Compute other things that we can't easily do in the database"""
    # replace any None values with np.nan
    df.fillna(value=np.nan, inplace=True)
    # contraversy here, drop any columns that are all missing
    # df.dropna(how='all', axis=1, inplace=True)


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    fmt = fields.get("_format", "json")
    network = fields.get('network', '')[:32]
    networkclass = fields.get('networkclass', '')[:32]
    wfo = fields.get('wfo', '')[:4]
    state = fields.get('state', '')[:2]
    event = fields.get('event')
    station = fields.getall('station')
    pgconn = get_dbconn('iem')
    if station:
        params = (tuple(station), )
        sql = SQL.replace("REPLACEME",
                          "t.id in %s and")
    elif networkclass != '' and wfo != '':
        params = (wfo, networkclass)
        sql = SQL.replace("REPLACEME",
                          "t.wfo = %s and t.network ~* %s and")
    elif wfo != '':
        params = (wfo, )
        sql = SQL.replace("REPLACEME",
                          "t.wfo = %s and")
    elif state != '':
        params = (state, )
        sql = SQL.replace("REPLACEME",
                          "t.state = %s and")
    elif network != '':
        sql = SQL.replace("REPLACEME",
                          "t.network = %s and")
        params = (network, )
    else:
        sql = SQL.replace("REPLACEME", "")
        params = []

    if fmt == 'geojson':
        df = read_postgis(sql, pgconn, params=params,
                          index_col='station', geom_col='geom')
    else:
        df = read_sql(sql, pgconn, params=params, index_col='station')
        df.drop('geom', axis=1, inplace=True)
    if event is not None and event in df.columns:
        df = df[df[event].notna()]
    compute(df)
    (tmpfd, tmpfn) = tempfile.mkstemp(text=True)
    os.close(tmpfd)
    if fmt == 'txt':
        df.to_csv(tmpfn, index=True)
    elif fmt == 'json':
        # Implement our 'table-schema' option
        return df
    elif fmt == 'geojson':
        df.to_file(tmpfn, driver="GeoJSON")

    return open(tmpfn).read()
