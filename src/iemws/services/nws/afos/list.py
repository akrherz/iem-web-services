"""List NWS Text Products Metadata.

This service returns metadata and hrefs for IEM stored NWS Text Products. The
`product_id` can be used to call `/api/1/nwstext/{product_id}` to retrieve the
actual text.

The provided `cccc` (WMO Source Code) can be provided as a three character
identifier.  In that case, a `K` is prepended to rectify it.

The provided `pil` (AFOS / AWIPS ID / 3-6 length identifier) can be an
explicit match or if it is three characters, a begining of `pil` match.  See
examples below for a better explaination.

This service omits any products with a PIL starting with `LLL` or `WRK`, which
are generally AWIPS internal products.

Sometimes multiple text products can exist within a single `product_id`.
Sadly, NWS directives are not always followed for how this is not supposed to
be an ambiguous situation.  The `count` attribute provides the number of
products that exist at the given `product_id`.

Examples
--------

 - `/api/1/nws/afos/list.json?cccc=KDMX&date=2022-10-28` : get all KDMX text
products for the UTC date of 28 Oct 2022.
 - `/api/1/nws/afos/list.json?pil=TOR&date=2022-10-28` : get all NWS for UTC
date of 28 Oct 2022 that have a awips / afos id starting with TOR.
 - `/api/1/nws/afos/list.json?cccc=KDMX&pil=TORDMX&date=2022-10-28` : get all
KDMX text TOR products for the UTC date of 28 Oct 2022.

"""

from datetime import date as dateobj
from datetime import timedelta

# Third Party
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.util import utc

# Local
from ....models import SupportedFormatsNoGeoJSON
from ....models.afos.list import AFOSListSchema
from ....util import deliver_df

ISO = "YYYY-MM-DDThh24:MI:SSZ"
router = APIRouter()


def handler(cccc, pil, dt):
    """Handle the request, return df."""
    sts = utc(dt.year, dt.month, dt.day)
    ets = sts + timedelta(days=1)
    # Cull out "faked" MOS for now
    plimiter = ""
    if cccc in [
        "KWNO",
    ]:
        plimiter = (
            "and substr(pil, 1, 3) not in ('NBS', 'NBX', 'NBE', 'NBH', "
            "'NBP', 'MAV', 'MET', 'MEX', 'LAV', 'LEV')"
        )
    params = {
        "cccc": cccc,
        "sts": sts,
        "ets": ets,
        "pil": pil,
    }
    fs = []
    if pil is not None:
        dfilter = "pil = :pil"
        if len(pil) == 3:
            dfilter = "substr(pil, 1, 3) = :pil"
        fs.append(dfilter)
    if cccc is not None:
        fs.append("source = :cccc")
    with get_sqlalchemy_conn("afos") as conn:
        # We don't auto-list some internal products like WRK LLL
        df = pd.read_sql(
            sql_helper(
                """
            select entered at time zone 'UTC' as entered, trim(pil) as pil,
            to_char(entered at time zone 'UTC', 'YYYYmmddHH24MI') || '-' ||
            source || '-' || wmo || '-' || trim(pil) ||
            (case when bbb is not null then '-' || bbb else '' end)
            as product_id, source as cccc, count(*)
            from products where {cols} and entered >= :sts
            and entered < :ets and substr(pil, 1, 3) not in ('WRK', 'LLL')
            {plimiter} GROUP by entered, pil, product_id, cccc
            ORDER by entered ASC
            """,
                cols=" and ".join(fs),
                plimiter=plimiter,
            ),
            conn,
            params=params,
            index_col=None,
        )
    if not df.empty:
        df["link"] = (
            "https://mesonet.agron.iastate.edu/p.php?pid=" + df["product_id"]
        )
        df["text_link"] = (
            "https://mesonet.agron.iastate.edu/api/1/nwstext/"
            + df["product_id"]
        )
    return df


@router.get(
    "/nws/afos/list.{fmt}",
    description=__doc__,
    response_model=AFOSListSchema,
    tags=[
        "nws",
    ],
)
def service(
    fmt: SupportedFormatsNoGeoJSON,
    cccc: str = Query(None, min_length=3, max_length=4),
    pil: str = Query(None, min_length=3, max_length=6),
    date: dateobj = Query(None),
):
    """Replaced above."""
    if date is None:
        date = utc()
    if cccc is None and pil is None:
        raise HTTPException(400, "Either cccc or pil must be set.")
    if cccc is not None:
        cccc = cccc.upper()
        if len(cccc) == 3:
            cccc = f"K{cccc}"
    if pil is not None:
        pil = pil.upper().strip()
    df = handler(cccc, pil, date)
    return deliver_df(df, fmt)


service.__doc__ = __doc__
