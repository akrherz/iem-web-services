"""/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX"""
import datetime

import pytz
from pyiem.util import get_dbconn


def handler(product_id):
    """Handle the request, return dict"""
    pgconn = get_dbconn("afos")
    cursor = pgconn.cursor()

    ts = datetime.datetime.strptime(product[:12], "%Y%m%d%H%M")
    ts = ts.replace(tzinfo=pytz.UTC)

    source = product_id[13:17]
    pil = product_id[25:]

    cursor.execute(
        """SELECT data from products where source = %s
    and pil = %s and entered = %s""",
        (source, pil, ts),
    )

    if cursor.rowcount == 0:
        return "Not Found %s %s" % (source, pil)

    row = cursor.fetchone()
    return row[0].replace("\r\r\n", "\n")
