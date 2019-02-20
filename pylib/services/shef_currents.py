"""Provide SHEF Currents for a given pe and duration."""
import os
import tempfile
import warnings

from pandas.io.sql import read_sql
from geopandas import read_postgis
from pyiem.util import get_dbconn
# prevent warnings that may trip up mod_wsgi
warnings.simplefilter('ignore')

CACHE_EXPIRE = 60
# Avoid three table aggregate by initial window join


def get_mckey(fields):
    """What's the key for this request"""
    return "%s_%s_%s" % (
        fields.get('pe', ''), fields.get('duration', ''),
        fields.get('days', '2')
    )


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    fmt = fields.get("_format", "json")
    pe = fields.get('pe', 'EP')[:2]
    duration = fields.get('duration', 'D')[:1]
    days = int(fields.get('days', 1))
    pgconn = get_dbconn('iem')
    sql = """
    WITH data as (
        SELECT c.station, c.valid, c.value,
        ST_x(geom) as lon, ST_Y(geom) as lat, geom,
        row_number() OVER (PARTITION by c.station) from
        current_shef c JOIN stations s on (c.station = s.id)
        WHERE physical_code = '%s' and duration = '%s' and
        valid >= now() - '%s days'::interval and value > -9999
    )
    SELECT station,
    to_char(valid at time zone 'UTC', 'YYYY-MM-DDThh24:MI:SSZ') as utc_valid,
    value, lon, lat, geom from data
    where row_number = 1
    """ % (pe, duration, days)

    if fmt == 'geojson':
        df = read_postgis(sql, pgconn, geom_col='geom')
    else:
        df = read_sql(sql, pgconn)
        df.drop('geom', axis=1, inplace=True)
    (tmpfd, tmpfn) = tempfile.mkstemp(text=True)
    os.close(tmpfd)
    if fmt == 'txt':
        df.to_csv(tmpfn, index=False)
    elif fmt == 'json':
        # Implement our 'table-schema' option
        return df
    elif fmt == 'geojson':
        df.to_file(tmpfn, driver="GeoJSON")

    return open(tmpfn).read()
