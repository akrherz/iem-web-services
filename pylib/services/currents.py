"""/api/1/currents.txt?network=IA_ASOS"""
import os
import tempfile

from pandas.io.sql import read_sql
from geopandas import read_postgis
from pyiem.util import get_dbconn

CACHE_EXPIRE = 60
SQL = """
    SELECT t.id as station, t.name,
    to_char(s.day, 'YYYY-mm-dd') as local_date,
    to_char(c.valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    to_char(c.valid at time zone t.tzname,
            'YYYY-MM-DDThh24:MI:SSZ') as local_valid,
    tmpf, max_tmpf, min_tmpf, dwpf,
    t.geom,
    ST_x(t.geom) as lon, ST_y(t.geom) as lat
    from current c, summary s, stations t WHERE
    t.network = %s and t.iemid = s.iemid and t.iemid = c.iemid
    and s.day = date(now() at time zone t.tzname)
"""


def get_mckey(fields):
    """What's the key for this request"""
    network = fields.get('network', '')
    return "%s" % (network,)


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    fmt = fields.get("_format", "json")
    network = fields.get('network', 'IA_ASOS')[:32]
    pgconn = get_dbconn('iem')

    if fmt == 'geojson':
        df = read_postgis(SQL, pgconn, params=(network, ),
                          index_col='station', geom_col='geom')
    else:
        df = read_sql(SQL, pgconn, params=(network, ), index_col='station')
        df.drop('geom', axis=1, inplace=True)
    (tmpfd, tmpfn) = tempfile.mkstemp()
    os.close(tmpfd)
    if fmt == 'txt':
        df.to_csv(tmpfn)
    elif fmt == 'json':
        # Implement our 'table-schema' option
        return df
    elif fmt == 'geojson':
        # does not work https://github.com/Toblerity/Fiona/issues/409
        df.to_file(tmpfn, driver="GeoJSON")

    return open(tmpfn).read()
