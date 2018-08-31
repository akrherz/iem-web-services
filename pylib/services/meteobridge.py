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
    if fields.get('key') not in [
            PROPS.get('meteobridge.key.OT0013'),
            PROPS.get('meteobridge.key.OT0014')]:
        return json.dumps("BAD_KEY")
    sid = ('OT0013'
           if PROPS.get('meteobridge.key.OT0013') == fields.get('key')
           else 'OT0014')
    now = datetime.datetime.utcnow()
    now = now.replace(tzinfo=pytz.UTC)
    ob = Observation(sid, 'OT', now)
    for fname in ['tmpf', 'max_tmpf', 'min_tmpf', 'dwpf', 'relh', 'sknt',
                  'pday', 'alti', 'drct']:
        if fields.get(fname, 'M') == 'M':
            continue
        ob.data[fname] = float(fields.get(fname))
    pgconn = get_dbconn('iem', user='mesonet')
    cursor = pgconn.cursor()
    ob.save(cursor)
    cursor.close()
    pgconn.commit()
    return json.dumps("OK")
