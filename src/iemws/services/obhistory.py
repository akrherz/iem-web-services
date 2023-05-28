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
from zoneinfo import ZoneInfo

import numpy as np
import pytz
from fastapi import APIRouter, HTTPException, Query
from metpy.calc import dewpoint_from_relative_humidity
from metpy.units import masked_array, units
from pandas.io.sql import read_sql
from pyiem.network import Table as NetworkTable

from ..models import SupportedFormatsNoGeoJSON
from ..models.obhistory import ObHistoryDataItem, ObHistorySchema
from ..util import deliver_df, get_dbconn

router = APIRouter()


def get_df(network, station, date):
    """Figure out how to get the data being requested."""
    if date == datetime.date.today() and network not in ["ISUSM"]:
        # Use IEM Access
        pgconn = get_dbconn("iem")
        return read_sql(
            "SELECT distinct valid at time zone 'UTC' as utc_valid, "
            "valid at time zone t.tzname as local_valid, tmpf, dwpf, sknt, "
            "drct, vsby, skyc1, skyl1, skyc2, skyl2, skyc3, skyl3, skyc4, "
            "skyl4, relh, feel, alti, mslp, phour, p03i, p24i, "
            "phour as p01i, raw, gust, max_tmpf_6hr, min_tmpf_6hr, "
            "array_to_string(wxcodes, ' ') as wxcodes, snowdepth "
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
    if network.find("_ASOS") > 0:
        # Use ASOS
        pgconn = get_dbconn("asos")
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct, "
            "vsby, skyc1, skyl1, skyc2, skyl2, skyc3, skyl3, skyc4, skyl4, "
            "relh, feel, alti, mslp, p01i, p03i, p24i, metar as raw, "
            "p03i, p06i, p24i, max_tmpf_6hr, min_tmpf_6hr, gust, "
            "array_to_string(wxcodes, ' ') as wxcodes, snowdepth "
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
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct, "
            "gust from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network in ["ISUSM", "ISUAG"]:
        # Use ISUAG
        pgconn = get_dbconn("isuag")
        df = read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, phour, "
            "valid at time zone %s as local_valid, tmpf, relh, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
        # Compute dew point
        if not df.empty:
            try:
                df["dwpf"] = (
                    dewpoint_from_relative_humidity(
                        masked_array(df["tmpf"].values, units("degF")),
                        masked_array(df["relh"].values, units("percent")),
                    )
                    .to(units("degF"))
                    .m
                )
            except TypeError:
                df["dwpf"] = np.nan
        return df
    if network in ["OT", "KCCI", "KELO", "KCRG", "KIMT", "SCAN"]:
        # lazy
        providers = {"OT": "other", "SCAN": "scan"}
        pgconn = get_dbconn(providers.get(network, "snet"))
        return read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpf, dwpf, sknt, drct "
            "from alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
    if network == "USCRN":
        pgconn = get_dbconn("other")
        df = read_sql(
            "SELECT valid at time zone 'UTC' as utc_valid, "
            "valid at time zone %s as local_valid, tmpc, rh, "
            "wind_mps from uscrn_alldata WHERE station = %s and "
            "valid >= %s and valid < %s ORDER by valid ASC",
            pgconn,
            params=(tzname, station, sts, ets),
            index_col=None,
        )
        if df.empty:
            return df
        # Do some unit work
        tmpc = masked_array(df["tmpc"].values, units("degC"))
        df["tmpf"] = tmpc.to(units("degF")).m
        if df["rh"].isna().all():
            df["dwpf"] = np.nan
        else:
            df["dwpf"] = (
                dewpoint_from_relative_humidity(
                    tmpc, masked_array(df["rh"].values, units("percent"))
                )
                .to(units("degF"))
                .m
            )
        if df["wind_mps"].isna().all():
            df["sknt"] = np.nan
        else:
            df["sknt"] = (
                masked_array(df["wind_mps"], units("meters per second"))
                .to(units("knots"))
                .m
            )
        return df
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
        df = df.pivot_table(
            index="utc_valid", columns="key", values="value", aggfunc="first"
        )
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
        if not df2.empty:
            df = df.merge(df2, on="utc_valid")

        # Generate the local_valid column
        df["local_valid"] = (
            df["utc_valid"]
            .dt.tz_localize(datetime.timezone.utc)
            .dt.tz_convert(tz)
        )
        return df
    return None


def compute(df, full):
    """Compute other things that we can't easily do in the database"""
    # simplify our timestamps to strings before exporting
    if not df.empty:
        df["utc_valid"] = df["utc_valid"].dt.strftime("%Y-%m-%dT%H:%MZ")
        df["local_valid"] = df["local_valid"].dt.strftime("%Y-%m-%dT%H:%M")
    # Make sure we have all columns
    if full:
        for item in ObHistoryDataItem.__fields__:
            if item not in df.columns:
                df[item] = np.nan
    # replace any None values with np.nan
    return df.fillna(value=np.nan)
    # contraversy here, drop any columns that are all missing
    # df.dropna(how='all', axis=1, inplace=True)


def handler(network, station, date, full):
    """Handle the request, return dict"""
    if date is None:
        date = datetime.date.today()
    df = get_df(network, station, date)
    if df is None:
        raise HTTPException(500, "No Data For Station.")
    # Run any addition calculations, if necessary
    return compute(df, full)


@router.get(
    "/obhistory.{fmt}",
    response_model=ObHistorySchema,
    description=__doc__,
    tags=[
        "iem",
    ],
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
    df = handler(network.upper(), station.upper(), date, full)
    return deliver_df(df, fmt)


# Not really used
service.__doc__ = __doc__
