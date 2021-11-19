"""IEM Daily Summary Service.

This API service returns IEM computed and network provided daily summary
information.  There are a number of ways to approach this app:

- `/api/1/daily.geojson?date=2021-06-02&network=IA_ASOS` - Get all IA_ASOS
stations for date in GeoJSON.
- `/api/1/daily.json?station=AMW&network=IA_ASOS&year=2021` - Get all Ames
ASOS data for 2021.
- `/api/1/daily.json?station=AMW&network=IA_ASOS&year=2021&month=6` - Get all
Ames ASOS data for June 2021.

Note that this service can emit GeoJSON, but sometimes that format does not
make much sense, for example when requesting just one station's worth of data.

"""
# stdlib
import datetime

# third party
from geopandas import read_postgis
from fastapi import Query, HTTPException, APIRouter
from ..models.daily import RootSchema
from ..models import SupportedFormats
from ..util import get_dbconn, deliver_df

router = APIRouter()


def get_df(network, station, date, month, year):
    """Handle the request, return dict"""
    if network.endswith("CLIMATE"):
        sl = ""
        if station is not None:
            sl = f" and station = '{station}'"
        dl = ""
        if date is not None:
            dl = f" and day = '{date:%Y-%m-%d}' "
        elif month is None and year is not None:
            dl = f" and year = {year} "
        elif month is not None and year is not None:
            dl = f" and year = '{year}' and month = '{month}'"
        df = read_postgis(
            f"""
            SELECT station, to_char(day, 'YYYY-mm-dd') as date,
            high as max_tmpf, low as min_tmpf,
            precip, null as max_gust, snow, snowd, null as min_rh,
            null as max_rh, null as max_dwpf, null as min_dwpf,
            null as min_feel, null as avg_feel, null as max_feel,
            null as max_gust_localts, null as max_drct,
            null as avg_sknt, null as vector_avg_drct,
            null as min_rstage, null as max_rstage,
            temp_hour, geom, id, name
            from alldata_{network[:2]} s JOIN stations t on (s.station = t.id)
            WHERE t.network = %s {sl} {dl}
            ORDER by day ASC, station ASC
            """,
            get_dbconn("coop"),
            params=(network,),
            geom_col="geom",
        )

    else:
        sl = ""
        if station is not None:
            sl = f" and id = '{station}'"
        dl = ""
        table = "summary"
        if date is not None:
            table = f"summary_{date:%Y}"
            dl = f" and day = '{date:%Y-%m-%d}' "
        elif month is None and year is not None:
            table = f"summary_{year}"
        elif month is not None and year is not None:
            table = f"summary_{year}"
            dt2 = (
                datetime.date(year, month, 1) + datetime.timedelta(days=35)
            ).replace(day=1)
            dl = f" and day >= '{year}-{month}-01' and day < '{dt2:%Y-%m-%d}'"
        df = read_postgis(
            f"""
            SELECT id as station, to_char(day, 'YYYY-mm-dd') as date,
            max_tmpf, min_tmpf, pday as precip, max_gust, snow, snowd,
            min_rh, max_rh, max_dwpf, min_dwpf, min_feel, avg_feel,
            max_feel, max_drct,
            max_gust_ts at time zone t.tzname as max_gust_localts,
            to_char(coop_valid at time zone t.tzname, 'HH24') as temp_hour,
            avg_sknt, vector_avg_drct,
            min_rstage, max_rstage,
            geom, id, name
            from {table} s JOIN stations t on (s.iemid = t.iemid)
            WHERE t.network = %s {sl} {dl}
            ORDER by day ASC, id ASC
            """,
            get_dbconn("iem"),
            params=(network,),
            geom_col="geom",
        )
    return df


@router.get("/daily.{fmt}", response_model=RootSchema, description=__doc__)
def service(
    fmt: SupportedFormats,
    network: str = Query(
        ..., description="IEM Network Identifier", max_length=20
    ),
    station: str = Query(
        None, description="IEM Station Identifier", max_length=20
    ),
    date: datetime.date = Query(
        None, description="Local station calendar date"
    ),
    month: int = Query(None, description="Local station month"),
    year: int = Query(None, description="Local station day"),
):
    """Replaced above with module __doc__"""
    if all(x is None for x in [station, date, month, year]):
        raise HTTPException(500, detail="Not enough arguments provided.")

    df = get_df(network, station, date, month, year)
    return deliver_df(df, fmt)


# Not really used
service.__doc__ = __doc__
