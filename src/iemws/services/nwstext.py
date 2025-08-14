"""Simple NWS Text Service.

This service emits a text file for a given IEM defined product ID. For example:
`/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX`

If parameter `nolimit` is unset, this is a one-shot service, so if the database
finds more than one entry for the provided identifier, a `X-IEM-Notice` header
is added to the response.  If you provide `nolimit`, then the service will
return the products seperated by \003 character.

The product_id's source and WMO TTAAII are omitted from the database source. In
general, the AFOS/AWIPS ID + bbb (if present) and timestamp is sufficient
to uniquely identify products.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Path, Query, Response
from pyiem.database import sql_helper, with_sqlalchemy_conn
from sqlalchemy.engine import Connection

from iemws.util import cache_control

router = APIRouter()


@with_sqlalchemy_conn("afos")
def handler(
    product_id, nolimit: bool, headers, conn: Connection | None = None
):
    """Handle the request, return dict"""
    tokens = product_id.split("-")
    bbb = None
    if len(tokens) == 4:
        (tstamp, _source, _ttaaii, pil) = tokens
    elif len(tokens) == 5:
        (tstamp, _source, _ttaaii, pil, bbb) = tokens
    else:
        raise HTTPException(
            status_code=404,
            detail="Invalid product_id format provided",
        )

    try:
        ts = datetime.strptime(tstamp, "%Y%m%d%H%M")
    except ValueError as exp:
        raise HTTPException(
            status_code=422,
            detail="Invalid timestamp provided",
        ) from exp
    ts = ts.replace(tzinfo=timezone.utc)

    params = {
        "pil": pil,
        "entered": ts,
        "bbb": bbb,
    }
    blim = "" if bbb is None else " and bbb = :bbb"
    # When bbb is unset, we can hit some ambiguity, so we prioritize the
    # entry that has no bbb
    rs = conn.execute(
        sql_helper(
            """
    SELECT data from products where pil = :pil and entered = :entered
    {blim} order by bbb ASC NULLS FIRST
        """,
            blim=blim,
        ),
        params,
    )

    if rs.rowcount == 0:
        raise HTTPException(status_code=404, detail="Product not found.")

    if rs.rowcount > 1:
        headers["X-IEM-Notice"] = "Multiple Products Found"
        if nolimit:
            res = []
            for row in rs:
                res.append(row[0].replace("\r\r\n", "\n"))
            return "\003".join(res)
    row = rs.fetchone()
    return row[0].replace("\r\r\n", "\n")


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
    nolimit: bool = Query(False, description="Return all products"),
):
    """Replaced above by __doc__."""
    headers = {}
    res = handler(product_id, nolimit, headers)
    return Response(res, headers=headers, media_type="text/plain")


nwstext_service.__doc__ = __doc__
