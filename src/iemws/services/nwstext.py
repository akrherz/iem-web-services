"""Simple NWS Text Service.

This service emits a text file for a given IEM defined product ID. For example:
`/api/1/nwstext/201410071957-KDMX-FXUS63-AFDDMX`

If parameter `nolimit` is unset, this is a one-shot service, so if the database
finds more than one entry for the provided identifier, a `X-IEM-Notice` header
is added to the response.  If you provide `nolimit`, then the service will
return the products seperated by \003 character.

The product_id's WMO TTAAII is omitted from the database search. In
general, the AFOS/AWIPS ID + bbb (if present) and timestamp is sufficient
to uniquely identify products.  The source is used to remove ambiguity, if
necessary.
"""

from datetime import datetime, timezone
from typing import Annotated

import psycopg
from fastapi import APIRouter, HTTPException, Path, Query, Response

from iemws.util import cache_control, get_async_conn

router = APIRouter()


async def handler(
    conn: psycopg.AsyncConnection,
    product_id: str,
    nolimit: bool,
    headers: dict,
):
    """Handle the request, return dict"""
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

    try:
        ts = datetime.strptime(tstamp, "%Y%m%d%H%M")
    except ValueError as exp:
        raise HTTPException(
            status_code=422,
            detail="Invalid timestamp provided",
        ) from exp
    ts = ts.replace(tzinfo=timezone.utc)

    args = [pil, ts]
    blim = ""
    if bbb is not None:
        blim = " and bbb = %s"
        args.append(bbb)
    # When bbb is unset, we can hit some ambiguity, so we prioritize the
    # entry that has no bbb
    rs = await conn.execute(
        f"""
    SELECT data, source from products where pil = %s and entered = %s
    {blim} order by bbb ASC NULLS FIRST
        """,
        args,
    )

    res = []
    res_all = []
    async for row in rs:
        payload = row[0].replace("\r", "")
        # Can we remove ambiguity by checking the source
        if row[1] == source:
            res.append(payload)
        res_all.append(payload)

    # If we found nothing, 404
    if not res_all:
        raise HTTPException(status_code=404, detail="Product not found.")

    # If the filtered result is len 1, we win
    if len(res) == 1:
        return res[0]

    # Now we have ambiguous returns
    headers["X-IEM-Notice"] = "Multiple Products Found"
    if nolimit:
        # At this point res is either len=0 or >1
        return "\003".join(res if len(res) > 1 else res_all)
    # Give up, more or less
    return res_all[0]


@router.get(
    "/nwstext/{product_id}",
    description=__doc__,
    tags=[
        "nws",
    ],
)
@cache_control(300)
async def nwstext_service(
    product_id: Annotated[str, Path(max_length=35, min_length=28)],
    nolimit: Annotated[bool, Query(description="Return all products")] = False,
):
    """Unused docstring."""
    headers = {}
    async with get_async_conn("afos") as conn:
        res = await handler(conn, product_id, nolimit, headers)
    return Response(res, headers=headers, media_type="text/plain")
