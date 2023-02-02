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
from logging.config import dictConfig
import warnings

from fastapi import FastAPI
from shapely.errors import ShapelyDeprecationWarning
from .config import LogConfig
from .services import (
    currents,
    cow,
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
    shef_currents,
    station,
    usdm_bypoint,
    scp,
    servertime,
    spc_watch_outline,
)
from .services.iemre import daily as iemre_daily
from .services.iemre import hourly as iemre_hourly
from .services.iemre import multiday as iemre_multiday
from .services.isusm import daily as isusm_daily
from .services.nws import (
    bufkit,
    current_flood_warnings,
    outlook_by_point,
    spc_mcd,
    spc_outlook,
    taf,
    taf_overview,
    ugcs,
    wpc_mpd,
    wpc_national_hilo,
)
from .services.nws.afos import list as nws_afos_list
from .services.vtec import county_zone, sbw_interval
from .util import handle_exception

# Stop a Shapely deprecation warning until geopandas is updated
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

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

dictConfig(LogConfig().dict())

app = FastAPI(
    root_path="/api/1",
    description=__doc__,
    title="IEM API v1",
    openapi_tags=tags_metadata,
)

# Unexpected Exception Handling, works in gunicorn, but not uvicorn??
app.add_exception_handler(Exception, handle_exception)

# The order here impacts the docs order
app.include_router(ffg_bypoint.router)

app.include_router(iemre_daily.router)
app.include_router(iemre_hourly.router)
app.include_router(iemre_multiday.router)

app.include_router(idot_dashcam.router)
app.include_router(idot_rwiscam.router)
app.include_router(iowa_winter_roadcond.router)
app.include_router(isusm_daily.router)
app.include_router(bufkit.router)
app.include_router(current_flood_warnings.router)
app.include_router(outlook_by_point.router)
app.include_router(daily.router)
app.include_router(taf.router)
app.include_router(taf_overview.router)
app.include_router(usdm_bypoint.router)
app.include_router(shef_currents.router)
app.include_router(obhistory.router)
app.include_router(last_shef.router)
app.include_router(mos.router)
app.include_router(network.router)
app.include_router(networks.router)
app.include_router(station.router)
app.include_router(nwstext.router)
app.include_router(meteobridge.router)
app.include_router(drydown.router)
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
app.include_router(sbw_interval.router)
app.include_router(ugcs.router)

app.include_router(servertime.router)
