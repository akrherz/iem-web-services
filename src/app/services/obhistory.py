"""IEM Observation History for One Date.

This service returns either a JSON or CSV formatted response with one day's
worth of observations as per the IEM Processing.  The day is a local calendar
date for the weather station.  Timestamps are returned in both UTC `utc_valid`
and local time `local_valid`.

When you request data for a HADS/COOP site, you get additional columns of
data back that include the explicit SHEF variable code.

The `full=boolean` parameter controls the number of variables returned.  The
default is to only return variables that the station/network supports. Setting
it to `true` means that each response contains the full domain of available
variables from this service even if the station does not report it.
"""
import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import pytz
import numpy as np
from pandas.io.sql import read_sql
from fastapi import Query, Response, HTTPException
from pyiem.network import Table as NetworkTable
from ..models.obhistory import RootSchema, DataItem
from ..models import SupportedFormatsNoGeoJSON
from ..util import get_dbconn
from ..reference import MEDIATYPES


def get_df(network, station, date):
    """Figure out how to get the data being requested."""
    if date == datetime.date.today():
        # Use IEM Access
        pgconn = get_dbconn("iem")
        return read_sql(
            "SELECT distinct valid at time zone 'UTC' as utc_valid, "
            "valid at time zone t.tzname as local_valid, tmpf, dwpf, sknt, "
            "drct, vsby, skyc1, skyl1, skyc2, skyl2, skyc3, skyl3, skyc4, "
            "skyl4, relh, feel, alti, mslp, phour, p03i, p24i, "
            "phour as p01i, raw, gust, "
            "array_to_string(wxcodes, ' ') as wxcodes "
            "from current_log c JOIN stations t on (c.iemid = t.iemid) "
            "WHERE t.id = %s and t.network = %s and "
            "date(valid at time zone t.tzname) = %s ORDER by utc_valid ASC",
            pgconn,
            params=(station, network, date),
            index_col=None,
        )
    nt = NetworkTable(network, only_online=False)
    if station not in nt.sts:
        raise HTTPException(404, "Station + Network unknown to the IEM.")
    tzname = nt.sts[station]["tzname"]
    # This sucks, but alas we want easy datetime construction
    tz = ZoneInfo(tzname)
    sts = datetime.datetime(date.year, date.month, date.day, tzinfo=tz)
    ets = sts + datetime.timedelta(hours=24)
    tz = pytz.timezone(tzname)
    if network.find("_ASOS") > 0 or network == "AWOS":
        # Use ASOS
        pgconn = get_dbconn("asos")
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct, "
            "vsby, skyc1, skyl1, skyc2, skyl2, skyc3, skyl3, skyc4, skyl4, "
            "relh, feel, alti, mslp, p01i, p03i, p24i, metar as raw, "
            "p03i, p06i, p24i, max_tmpf_6hr, min_tmpf_6hr, gust, "
            "array_to_string(wxcodes, ' ') as wxcodes "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network.find("_RWIS") > 0:
        # Use RWIS
        pgconn = get_dbconn("rwis")
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network in ["ISUSM", "ISUAG"]:
        # Use ISUAG
        pgconn = get_dbconn("isuag")
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network == "OT":
        # Use ISUAG
        pgconn = get_dbconn("other")
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network.find("_COOP") > 0 or network.find("_DCP") > 0:
        # Use HADS
        pgconn = get_dbconn("hads")
        df = read_sql(
            "SELECT distinct valid at time zone 'UTC' as utc_valid, "
            "key, value "
            f"from raw{date.strftime('%Y')} WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by utc_valid ASC",
            pgconn,
            params=(station, sts, ets),
            index_col=None,
        )
        if df.empty:
            return df
        df = df.pivot(index="utc_valid", columns="key", values="value")
        df = df.reset_index()
        # Query alldata too as it has the variable conversions done
        df2 = read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "tmpf, dwpf, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by utc_valid ASC",
            pgconn,
            params=(station, sts, ets),
            index_col=None,
        )
        df = df.merge(df2, on="utc_valid")

        # Generate the local_valid column
        df["local_valid"] = (
            df["utc_valid"]
            .dt.tz_localize(datetime.timezone.utc)
            .dt.tz_convert(tz)
        )
        return df


def compute(df, full):
    """Compute other things that we can't easily do in the database"""
    # simplify our timestamps to strings before exporting
    if not df.empty:
        df["utc_valid"] = df["utc_valid"].dt.strftime("%Y-%m-%dT%H:%MZ")
        df["local_valid"] = df["local_valid"].dt.strftime("%Y-%m-%dT%H:%M")
    # Make sure we have all columns
    if full:
        for item in DataItem.__fields__:
            if item not in df.columns:
                df[item] = np.nan
    # replace any None values with np.nan
    return df.fillna(value=np.nan)
    # contraversy here, drop any columns that are all missing
    # df.dropna(how='all', axis=1, inplace=True)


def handler(network, station, date, full, fmt):
    """Handle the request, return dict"""
    if date is None:
        date = datetime.date.today()
    df = get_df(network, station, date)
    # Run any addition calculations, if necessary
    df = compute(df, full)
    if fmt == SupportedFormatsNoGeoJSON.txt:
        return df.to_csv(index=False)
    # Implement our 'table-schema' option
    return df.to_json(orient="table", default_handler=str, index=False)


def factory(app):
    """Generate the app."""

    @app.get(
        "/obhistory.{fmt}", response_model=RootSchema, description=__doc__
    )
    def service(
        fmt: SupportedFormatsNoGeoJSON,
        network: str = Query(
            ..., description="IEM Network Identifier", max_length=20
        ),
        station: str = Query(
            ..., description="IEM Station Identifier", max_length=20
        ),
        date: datetime.date = Query(
            None, description="Local station calendar date"
        ),
        full: bool = Query(False, description="Include all variables?"),
    ):
        """Replaced above with module __doc__"""

        return Response(
            handler(network.upper(), station.upper(), date, full, fmt),
            media_type=MEDIATYPES[fmt],
        )

    # Not really used
    service.__doc__ = __doc__
