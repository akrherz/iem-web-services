"""Simple NWS Text Service.

This service emits a text file for a given IEM defined product ID. For example:
`/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX`

This is a one-shot service, so if the database finds more than one entry for
the provided identifier, a `X-IEM-Notice` header is added to the response.
"""

import datetime

import pytz
from fastapi import APIRouter, HTTPException, Path, Response
from pyiem.database import get_dbconnc

from iemws.util import cache_control

router = APIRouter()


def handler(product_id, headers):
    """Handle the request, return dict"""
    pgconn, cursor = get_dbconnc("afos")
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
    # When bbb is unset, we can hit some ambiguity, so we prioritize the
    # entry that has no bbb
    cursor.execute(
        "SELECT data from products where source = %s "
        f"and pil = %s and entered = %s {extra} "
        "order by bbb ASC NULLS FIRST",
        args,
    )

    if cursor.rowcount == 0:
        pgconn.close()
        raise HTTPException(status_code=404, detail="Product not found.")

    if cursor.rowcount > 1:
        headers["X-IEM-Notice"] = "Multiple Products Found"

    row = cursor.fetchone()
    pgconn.close()
    return row["data"].replace("\r\r\n", "\n")


@router.get(
    "/nwstext/{product_id}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
@cache_control(300)
def nwstext_service(
    product_id: str = Path(..., max_length=35, min_length=28),
):
    """Replaced above by __doc__."""
    headers = {}
    res = handler(product_id, headers)
    return Response(res, headers=headers, media_type="text/plain")


nwstext_service.__doc__ = __doc__
