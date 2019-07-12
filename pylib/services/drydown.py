"""Provide data to support drydown app."""
import warnings
import json
from io import StringIO

from metpy.units import units
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn
from pyiem.iemre import get_gid
# prevent warnings that may trip up mod_wsgi
warnings.simplefilter('ignore')

CACHE_EXPIRE = 3600
# Avoid three table aggregate by initial window join


def get_mckey(fields):
    """What's the key for this request"""
    return "%s_%s" % (
        fields.get('lat', ''), fields.get('lon', '')
    )


def handler(_version, fields, _environ):
    """Handle the request."""
    # fmt = fields.get("_format", "json")
    lat = float(fields.get('lat', '42.0'))
    lon = float(fields.get('lon', '-95.0'))
    gid = get_gid(lon, lat)

    pgconn = get_dbconn('iemre')
    df = read_sql("""
        SELECT valid, high_tmpk, low_tmpk, (max_rh - min_rh) / 2 as avg_rh
        from iemre_daily WHERE gid = %s and valid > '1980-01-01' and
        to_char(valid, 'mmdd') between '0901' and '1101'
        ORDER by valid ASC
    """, pgconn, params=(int(gid), ), parse_dates='valid', index_col=None)
    df['max_tmpf'] = (df['high_tmpk'].values * units.degK).to(units.degF).m
    df['min_tmpf'] = (df['low_tmpk'].values * units.degK).to(units.degF).m
    df['avg_rh'] = df['avg_rh'].fillna(50)

    df['year'] = df['valid'].dt.year
    res = {'data': {}}
    for year, df2 in df.groupby('year'):
        res['data'][year] = {
            'dates': df2['valid'].dt.strftime("%Y-%m-%d").values.tolist(),
            'high': df2['max_tmpf'].values.astype('i').tolist(),
            'low': df2['min_tmpf'].values.astype('i').tolist(),
            'rh': df2['avg_rh'].values.astype('i').tolist()}
    sio = StringIO()
    json.dump(res, sio)
    sio.seek(0)
    return sio.read()
