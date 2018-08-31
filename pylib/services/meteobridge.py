"""Something simple"""
import json
import sys
import datetime

import pytz
from pyiem.util import get_properties, get_dbconn, utc
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
    if len(fields.get('time', '')) == 14:
        _t = fields.get('time')
        now = utc(int(_t[:4]), int(_t[4:6]), int(_t[6:8]), int(_t[8:10]),
                  int(_t[10:12]), int(_t[12:14]))
    else:
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
