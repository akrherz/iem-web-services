"""Storm Prediction Center Watch Outlines"""
import datetime
import json

import pytz
from geopandas import read_postgis
from pyiem.util import get_dbconn

ISO = "%Y-%m-%dT%H:%M:%SZ"


def get_mckey(fields):
    """What's the key for this request"""
    valid = fields.get('valid', '')
    return "%s" % (valid, )


def make_date(val):
    """Convert the value into a date please"""
    if val == '':
        return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    return datetime.datetime.strptime(val[:16], '%Y-%m-%dT%H:%M').replace(
        tzinfo=pytz.UTC)


def run(valid):
    """Do the work, please"""
    pgconn = get_dbconn('postgis')
    valid = make_date(valid)

    df = read_postgis("""
        SELECT sel,
        to_char(issued at time zone 'UTC',
                'YYYY-MM-DDThh24:MI:SSZ') as utc_issued,
        to_char(expired at time zone 'UTC',
                'YYYY-MM-DDThh24:MI:SSZ') as utc_expired,
        type, num, geom from watches WHERE
        issued <= %s and expired > %s
        ORDER by issued ASC
    """, pgconn, params=(valid, valid), index_col=None, geom_col='geom')

    return df


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    valid = fields.get('valid', '')
    fmt = fields.get("_format", "json")
    df = run(valid)
    if fmt == 'geojson':
        jobj = json.loads(df.to_json())
        jobj['generation_time'] = datetime.datetime.utcnow().strftime(ISO)
        return json.dumps(jobj)
    return df
