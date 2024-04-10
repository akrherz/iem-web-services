"""
Return to [IEM API Homepage](https://mesonet.agron.iastate.edu/api/).

This answers `/api/1/` versioned requests against the IEM.  This service is
driven by the awesome [FastAPI](https://fastapi.tiangolo.com/) Python library.

**Philosophy** - I am not the sharpest tool in the shed, but I am trying things
as I figure things out.  API design is hard and I tend to want to move quickly,
so this is what you get.  Whilst REST principles are interesting, I find that
they are difficult for folks to use that are more scientists than programmers.
The URI endpoints do encapsulate the return data format by the suffix on
the last path segment.  When you see the ``{fmt}`` suffix, that means the
service supports various return formats.  Otherwise, only the shown option
is available.  I am always interested in learning things and if you are
greatly offended by this, please let me know!
daryl herzmann [akrherz@iastate.edu](mailto:akrherz@iastate.edu)

**Scalability** - These services and the backend server answering the
requests have finite capacity.  I would suggest not using these on a highly
trafficked website, nor launch a AWS region of EC2 instances against me.

**Tracking Status/Outages** - I usually alert folks to server issues via my
[twitter account](https://github.com/akrherz).  Subscribing to the IEM's
[RSS feed](https://mesonet.agron.iastate.edu/rss.php) is also a good
place to see what I am up to.

**Terms of Usage** - This service is free to use for most any legal purpose.
Please don't sue Iowa State University when daryl herzmann gets hit by a bus
someday and then entire IEM goes away!
"""

import threading
import time
import warnings
from collections import namedtuple
from datetime import timedelta
from logging.config import dictConfig
from queue import Queue

import pandas as pd
from fastapi import FastAPI, Request
from pyiem.util import LOG, get_dbconn, utc
from shapely.errors import ShapelyDeprecationWarning

from .config import log_config
from .services import (
    climodat,
    cow,
    currents,
    daily,
    drydown,
    ffg_bypoint,
    idot_dashcam,
    idot_rwiscam,
    iowa_winter_roadcond,
    last_shef,
    meteobridge,
    mos,
    network,
    networks,
    nwstext,
    obhistory,
    raobs_by_year,
    scp,
    servertime,
    shef_currents,
    spc_watch_outline,
    station,
    usdm_bypoint,
)
from .services.iem import trending_autoplots
from .services.iemre import daily as iemre_daily
from .services.iemre import hourly as iemre_hourly
from .services.iemre import multiday as iemre_multiday
from .services.isusm import daily as isusm_daily
from .services.nws import (
    bufkit,
    centers_for_point,
    current_flood_warnings,
    emergencies,
    lsrs_by_point,
    outlook_by_point,
    snowfall_6hour,
    spc_mcd,
    spc_outlook,
    taf,
    taf_overview,
    ugcs,
    wpc_mpd,
    wpc_national_hilo,
)
from .services.nws.afos import list as nws_afos_list
from .services.vtec import county_zone, events_status, sbw_interval
from .util import handle_exception

# Stop a Shapely deprecation warning until geopandas is updated
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)
# Stop a Pandas warning about future silent downcasting
pd.set_option("future.no_silent_downcasting", True)

# Order here controls the order of the API documentation
tags_metadata = [
    {
        "name": "iem",
        "description": "IEM Centric APIs",
    },
    {
        "name": "nws",
        "description": "National Weather Service (NWS) APIs by IEM",
    },
    {
        "name": "vtec",
        "description": (
            "National Weather Service (NWS) VTEC Watch Warning Advisory APIs"
        ),
    },
    {
        "name": "debug",
        "description": "Test and Debugging APIs",
    },
]

dictConfig(log_config)
# Queue for writing telemetry data to database
TELEMETRY_QUEUE = Queue()
TELEMETRY_QUEUE_THREAD = {"worker": None, "dbconn": None, "lasterr": utc(1980)}
TELEMETRY = namedtuple(
    "TELEMETRY",
    ["timing", "status_code", "client_addr", "app", "request_uri"],
)


def _writer(data):
    """Actually write the data."""
    if TELEMETRY_QUEUE_THREAD["dbconn"] is None:
        TELEMETRY_QUEUE_THREAD["dbconn"] = get_dbconn("mesosite")
    cursor = TELEMETRY_QUEUE_THREAD["dbconn"].cursor()
    cursor.execute(
        """
        insert into website_telemetry(timing, status_code, client_addr,
        app, request_uri) values (%s, %s, %s, %s, %s)
        """,
        (
            data.timing,
            data.status_code,
            data.client_addr,
            data.app,
            data.request_uri,
        ),
    )
    cursor.close()
    TELEMETRY_QUEUE_THREAD["dbconn"].commit()


def _writer_thread():
    """Runs for ever and writes telemetry data to the database."""
    while True:
        data = TELEMETRY_QUEUE.get()

        try:
            if utc() > TELEMETRY_QUEUE_THREAD["lasterr"]:
                _writer(data)
        except Exception as exp:
            TELEMETRY_QUEUE_THREAD["lasterr"] = utc() + timedelta(minutes=5)
            LOG.exception(exp)
            TELEMETRY_QUEUE_THREAD["dbconn"] = None


def _add_to_queue(data):
    """Adds data to queue, ensures a thread is running to process."""
    if TELEMETRY_QUEUE_THREAD["worker"] is None:
        TELEMETRY_QUEUE_THREAD["worker"] = threading.Thread(
            target=_writer_thread,
            name="telemetry",
            daemon=True,
        )
        TELEMETRY_QUEUE_THREAD["worker"].start()
    TELEMETRY_QUEUE.put(data)


app = FastAPI(
    root_path="/api/1",
    description=__doc__,
    title="IEM API v1",
    openapi_tags=tags_metadata,
)


@app.middleware("http")
async def record_request_timing(request: Request, call_next):
    """
    A middleware to record request timing.
    """
    start_time = time.time()
    response = await call_next(request)
    # within pytest, request.client is None
    clienthost = None if request.client is None else request.client.host
    remote_addr = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or clienthost
    )
    _add_to_queue(
        TELEMETRY(
            time.time() - start_time,
            response.status_code,
            "127.0.0.1" if remote_addr == "testclient" else remote_addr,
            request.url.path,
            request.url.path + "?" + request.url.query,
        )
    )

    return response


# Unexpected Exception Handling, works in gunicorn, but not uvicorn??
app.add_exception_handler(Exception, handle_exception)

# The order here impacts the docs order
app.include_router(ffg_bypoint.router)

app.include_router(iemre_daily.router)
app.include_router(iemre_hourly.router)
app.include_router(iemre_multiday.router)
app.include_router(trending_autoplots.router)

app.include_router(climodat.router)
app.include_router(idot_dashcam.router)
app.include_router(idot_rwiscam.router)
app.include_router(iowa_winter_roadcond.router)
app.include_router(isusm_daily.router)
app.include_router(bufkit.router)
app.include_router(centers_for_point.router)
app.include_router(current_flood_warnings.router)
app.include_router(emergencies.router)
app.include_router(outlook_by_point.router)
app.include_router(daily.router)
app.include_router(taf.router)
app.include_router(taf_overview.router)
app.include_router(usdm_bypoint.router)
app.include_router(shef_currents.router)
app.include_router(obhistory.router)
app.include_router(last_shef.router)
app.include_router(lsrs_by_point.router)
app.include_router(mos.router)
app.include_router(network.router)
app.include_router(networks.router)
app.include_router(station.router)
app.include_router(nwstext.router)
app.include_router(meteobridge.router)
app.include_router(drydown.router)
app.include_router(snowfall_6hour.router)
app.include_router(spc_mcd.router)
app.include_router(spc_outlook.router)
app.include_router(wpc_mpd.router)
app.include_router(wpc_national_hilo.router)
app.include_router(cow.router)
app.include_router(currents.router)
app.include_router(raobs_by_year.router)
app.include_router(scp.router)
app.include_router(spc_watch_outline.router)
app.include_router(nws_afos_list.router)

app.include_router(county_zone.router)
app.include_router(events_status.router)
app.include_router(sbw_interval.router)
app.include_router(ugcs.router)

app.include_router(servertime.router)
