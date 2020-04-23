"""Simple NWS Text Service.

This service emits a text file for a given IEM defined product ID. For example:
`/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX`"""
import datetime

import pytz
from fastapi import Query, Response
from ..util import get_dbconn


def handler(product_id):
    """Handle the request, return dict"""
    pgconn = get_dbconn("afos")
    cursor = pgconn.cursor()

    ts = datetime.datetime.strptime(product_id[:12], "%Y%m%d%H%M")
    ts = ts.replace(tzinfo=pytz.UTC)

    source = product_id[13:17]
    pil = product_id[25:]

    cursor.execute(
        "SELECT data from products where source = %s "
        "and pil = %s and entered = %s",
        (source, pil, ts),
    )

    if cursor.rowcount == 0:
        return "Not Found %s %s" % (source, pil)

    row = cursor.fetchone()
    return row[0].replace("\r\r\n", "\n")


def factory(app):
    """Generate."""

    @app.get("/nwstext/{product_id}", description=__doc__)
    def nwstext_service(
        product_id: str = Query(..., max_length=31, min_length=31),
    ):
        """Replaced above by __doc__."""
        return Response(handler(product_id), media_type="text/plain")

    nwstext_service.__doc__ = __doc__
