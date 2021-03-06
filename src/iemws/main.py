"""
Return to [IEM API Homepage](https://mesonet.agron.iastate.edu/api/).

This answers `/api/1/` versioned requests against the IEM.  This service is
driven by the awesome [FastAPI](https://fastapi.tiangolo.com/) Python library.

"""

from fastapi import FastAPI
from .services import (
    currents,
    cow,
    drydown,
    ffg_bypoint,
    idot_dashcam,
    meteobridge,
    mos,
    nwstext,
    obhistory,
    raobs_by_year,
    shef_currents,
    usdm_bypoint,
    scp,
    servertime,
    spc_watch_outline,
)

app = FastAPI(root_path="/api/1", description=__doc__, title="IEM API v1")

# /servertime
servertime.factory(app)

# /ffg_bypoint.geojson
ffg_bypoint.factory(app)

# /idot_dashcam.{fmt}
idot_dashcam.factory(app)

# /usdm_bypoint.json
usdm_bypoint.factory(app)

# /shef_currents.{fmt}
shef_currents.factory(app)

# /obhistory.{fmt}
obhistory.factory(app)

# /mos.{fmt}
mos.factory(app)

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
