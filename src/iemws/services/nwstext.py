"""Simple NWS Text Service.

This service emits a text file for a given IEM defined product ID. For example:
`/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX`"""
import datetime

import pytz
from fastapi import Query, Response, HTTPException, APIRouter
from ..util import get_dbconn

router = APIRouter()


def handler(product_id):
    """Handle the request, return dict"""
    pgconn = get_dbconn("afos")
    cursor = pgconn.cursor()
    tokens = product_id.split("-")
    bbb = None
    if len(tokens) == 4:
        (tstamp, source, _ttaaii, pil) = tokens
    elif len(tokens) == 5:
        (tstamp, source, _ttaaii, pil, bbb) = tokens
    else:
        raise HTTPException(
            status_code=404,
            detail="Invalid product_id format provided",
        )

    ts = datetime.datetime.strptime(tstamp, "%Y%m%d%H%M")
    ts = ts.replace(tzinfo=pytz.UTC)

    args = [source, pil, ts]
    extra = ""
    if bbb:
        extra = " and bbb = %s"
        args.append(bbb)
    cursor.execute(
        "SELECT data from products where source = %s "
        f"and pil = %s and entered = %s {extra}",
        args,
    )

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Product not found.")

    row = cursor.fetchone()
    return row[0].replace("\r\r\n", "\n")


@router.get("/nwstext/{product_id}", description=__doc__)
def nwstext_service(
    product_id: str = Query(..., max_length=35, min_length=28),
):
    """Replaced above by __doc__."""
    return Response(handler(product_id), media_type="text/plain")


nwstext_service.__doc__ = __doc__
