"""/api/1/nwstext.txt?pid=201410071957-KDMX-FXUS63-AFDDMX"""
import datetime

import pytz
from pyiem.util import get_dbconn


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    product = fields.get("pid", "201410071957-KDMX-FXUS63-AFDDMX")[:32]
    pgconn = get_dbconn("afos")
    cursor = pgconn.cursor()

    ts = datetime.datetime.strptime(product[:12], "%Y%m%d%H%M")
    ts = ts.replace(tzinfo=pytz.utc)

    source = product[13:17]
    pil = product[25:]

    cursor.execute(
        """SELECT data from products where source = %s
    and pil = %s and entered = %s""",
        (source, pil, ts),
    )

    if cursor.rowcount == 0:
        return "Not Found %s %s" % (source, pil)

    row = cursor.fetchone()
    return row[0].replace("\r\r\n", "\n")
