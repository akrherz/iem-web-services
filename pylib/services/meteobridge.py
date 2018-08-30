"""Something simple"""
import json
import sys
import datetime

import pytz
from pyiem.util import get_properties, get_dbconn
from pyiem.observation import Observation

PROPS = {}


def handler(version, fields, environ):
    """Handle the request, return dict"""
    sys.stderr.write(repr(fields))
    if not PROPS:
        sys.stderr.write("Loading properties()...\n")
        PROPS.update(get_properties())
    if fields.get('key') != PROPS.get('meteobridge.key.OT0013'):
        return json.dumps("BAD_KEY")
    now = datetime.datetime.utcnow()
    now = now.replace(tzinfo=pytz.UTC)
    ob = Observation('OT0013', 'OT', now)
    for fname in ['tmpf', 'max_tmpf', 'min_tmpf', 'dwpf', 'relh', 'sknt',
                  'pday', 'pres', 'drct']:
        if fields.get(fname, 'M') == 'M':
            continue
        ob.data[fname] = float(fields.get(fname))
    pgconn = get_dbconn('iem', user='mesonet')
    cursor = pgconn.cursor()
    ob.save(cursor)
    cursor.close()
    pgconn.commit()
    return json.dumps("OK")
