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
from .services.isusm import daily as isusm_daily
from .services.nws import current_flood_warnings, bufkit, taf, taf_overview

app = FastAPI(root_path="/api/1", description=__doc__, title="IEM API v1")

# /servertime
servertime.factory(app)

# /ffg_bypoint.geojson
ffg_bypoint.factory(app)

# /idot_dashcam.{fmt}
idot_dashcam.factory(app)

# /iowa_winter_roadcond.{fmt}
iowa_winter_roadcond.factory(app)

# /isusm/daily.{fmt}
isusm_daily.factory(app)

# /nws/bufkit.{fmt}
bufkit.factory(app)

# /nws/current_flood_warnings.{fmt}
current_flood_warnings.factory(app)

# /daily.{fmt}
daily.factory(app)

# /nws/taf.{fmt}
taf.factory(app)

# /nws/taf_overview.{fmt}
taf_overview.factory(app)

# /usdm_bypoint.json
usdm_bypoint.factory(app)

# /shef_currents.{fmt}
shef_currents.factory(app)

# /obhistory.{fmt}
obhistory.factory(app)

# /mos.{fmt}
mos.factory(app)

# /network/{network}.{fmt}
network.factory(app)

# /networks.{fmt}
networks.factory(app)

# /nwstext/{product_id}
nwstext.factory(app)

# /meteobridge.json
meteobridge.factory(app)

# /drydown.json
drydown.factory(app)

# /cow.json
cow.factory(app)

# /currents.{fmt}
currents.factory(app)

# /raobs_by_year.json
raobs_by_year.factory(app)

# /scp.json
scp.factory(app)

# /spc_watch_outline.geojson
spc_watch_outline.factory(app)
