"""US Drought Monitor by lat/lon point"""
import datetime

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn

ISO = "%Y-%m-%dT%H:%M:%SZ"


def get_mckey(fields):
    """What's the key for this request"""
    sdate = fields.get('sdate', '')
    edate = fields.get('edate', '')
    lat = float(fields.get('lat', 42.0))
    lon = float(fields.get('lon', -95.))
    return "%s/%s/%s/%s" % (sdate, edate, lon, lat)


def make_date(val, default):
    """Convert the value into a date please"""
    if val == '':
        return default
    return datetime.datetime.strptime(val, '%Y-%m-%d')


def run(sdate, edate, lon, lat):
    """Do the work, please"""
    pgconn = get_dbconn('postgis')
    sdate = make_date(sdate, datetime.date(2000, 1, 1))
    edate = make_date(edate, datetime.date(2050, 1, 1))

    df = read_sql("""
        SELECT to_char(valid, 'YYYY-MM-DD') as valid,
        max(dm) as category from usdm WHERE
        ST_Contains(geom, ST_SetSRID(ST_GeomFromEWKT('POINT(%s %s)'),4326))
        and valid >= %s and valid <= %s
        GROUP by valid ORDER by valid ASC
    """, pgconn, params=(lon, lat, sdate, edate), index_col=None)

    return df


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    sdate = fields.get('sdate', '')
    edate = fields.get('edate', '')
    lat = float(fields.get('lat', 42.0))
    lon = float(fields.get('lon', -95.))

    return run(sdate, edate, lon, lat)
