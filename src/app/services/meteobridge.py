"""Something simple"""
import sys
import datetime

import pytz
from pyiem.util import get_properties, get_dbconn, utc
from pyiem.observation import Observation

PROPS = {}


def handler(
    key, time, tmpf, max_tmpf, min_tmpf, dwpf, relh, sknt, pday, alti, drct
):
    """Handle the request, return dict"""
    # sys.stderr.write(repr(fields))
    if not PROPS:
        sys.stderr.write("Loading properties()...\n")
        PROPS.update(get_properties())
    lookup = {}
    for sid in ["OT0013", "OT0014", "OT0015"]:
        lookup[PROPS.get("meteobridge.key." + sid)] = sid
    if key not in lookup:
        return "BAD_KEY"
    sid = lookup[key]
    if len(time) == 14:
        _t = time
        now = utc(
            int(_t[:4]),
            int(_t[4:6]),
            int(_t[6:8]),
            int(_t[8:10]),
            int(_t[10:12]),
            int(_t[12:14]),
        )
    else:
        now = datetime.datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
    ob = Observation(sid, "OT", now)
    for fname in [
        "tmpf",
        "max_tmpf",
        "min_tmpf",
        "dwpf",
        "relh",
        "sknt",
        "pday",
        "alti",
        "drct",
    ]:
        if vars()[fname] == "M":
            continue
        ob.data[fname] = float(vars()[fname])
    pgconn = get_dbconn("iem", user="mesonet")
    cursor = pgconn.cursor()
    ob.save(cursor)
    cursor.close()
    pgconn.commit()
    return "OK"
