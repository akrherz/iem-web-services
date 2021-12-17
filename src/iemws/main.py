"""
Return to [IEM API Homepage](https://mesonet.agron.iastate.edu/api/).

This answers `/api/1/` versioned requests against the IEM.  This service is
driven by the awesome [FastAPI](https://fastapi.tiangolo.com/) Python library.

"""

from fastapi import FastAPI
from .services import (
    currents,
    cow,
    daily,
    drydown,
    ffg_bypoint,
    idot_dashcam,
    iowa_winter_roadcond,
    meteobridge,
    mos,
    network,
    networks,
    nwstext,
    obhistory,
    raobs_by_year,
    shef_currents,
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
    spc_mcd,
    taf,
    taf_overview,
)

app = FastAPI(root_path="/api/1", description=__doc__, title="IEM API v1")

# The order here impacts the docs order
app.include_router(currents.router)
app.include_router(servertime.router)
app.include_router(ffg_bypoint.router)

app.include_router(iemre_daily.router)
app.include_router(iemre_hourly.router)
app.include_router(iemre_multiday.router)

app.include_router(idot_dashcam.router)
app.include_router(iowa_winter_roadcond.router)
app.include_router(isusm_daily.router)
app.include_router(bufkit.router)
app.include_router(current_flood_warnings.router)
app.include_router(daily.router)
app.include_router(taf.router)
app.include_router(taf_overview.router)
app.include_router(usdm_bypoint.router)
app.include_router(shef_currents.router)
app.include_router(obhistory.router)
app.include_router(mos.router)
app.include_router(network.router)
app.include_router(networks.router)
app.include_router(nwstext.router)
app.include_router(meteobridge.router)
app.include_router(drydown.router)
app.include_router(spc_mcd.router)
app.include_router(cow.router)
app.include_router(currents.router)
app.include_router(raobs_by_year.router)
app.include_router(scp.router)
app.include_router(spc_watch_outline.router)
