"""Our Python FastAPI server!

This answers /api/1/ versioned requests against the IEM.
"""
from typing import List
from datetime import datetime, date

from fastapi import FastAPI, Query
from fastapi.responses import Response
from .services import (
    currents,
    cow,
    drydown,
    meteobridge,
    nwstext,
    shef_currents,
    usdm_bypoint,
)

app = FastAPI(openapi_prefix="/api/1")


@app.get("/servertime")
def time_service():
    """Babysteps."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


@app.get("/usdm_bypoint")
def usdm_bypoint_service(
    sdate: date = Query(...),
    edate: date = Query(...),
    lon: float = Query(...),
    lat: float = Query(...),
):
    """Babysteps."""
    return usdm_bypoint.handler(sdate, edate, lon, lat)


@app.get("/shef_currents")
def shef_currents_service(
    fmt: str = Query(...),
    pe: str = Query(..., max_length=2),
    duration: str = Query(..., max_length=1),
    days: int = Query(1),
):
    """Babysteps."""
    mediatypes = {
        "json": "application/json",
        "geojson": "application/vnd.geo+json",
        "txt": "text/plain",
    }
    return Response(
        shef_currents.handler(fmt, pe, duration, days),
        media_type=mediatypes[fmt],
    )


@app.get("/nwstext/{product_id}")
def nwstext_service(
    product_id: str = Query(..., max_length=31, min_length=31),
):
    """Babysteps."""
    return Response(nwstext.handler(product_id), media_type="text/plain")


@app.get("/meteobridge.json")
def meteobridge_service(
    key: str = Query(...),
    time: str = Query(...),
    tmpf: str = Query(...),
    max_tmpf: str = Query(...),
    min_tmpf: str = Query(...),
    dwpf: str = Query(...),
    relh: str = Query(...),
    sknt: str = Query(...),
    pday: str = Query(...),
    alti: str = Query(...),
    drct: str = Query(...),
):
    """Babysteps."""
    return meteobridge.handler(
        key, time, tmpf, max_tmpf, min_tmpf, dwpf, relh, sknt, pday, alti, drct
    )


@app.get("/drydown")
def drydown_service(lat: float = Query(...), lon: float = Query(...)):
    """Babysteps."""
    return drydown.handler(lon, lat)


@app.get("/cow")
def cow_service(
    wfo: List[str] = Query(..., max_length=4),
    begints: datetime = Query(...),
    endts: datetime = Query(...),
    phenomena: List[str] = Query(None, max_length=2),
    lsrtype: List[str] = Query(None, max_length=2),
    hailsize: float = Query(1),
    lsrbuffer: float = Query(15),
    warningbuffer: float = Query(1),
    wind: float = Query(58),
    windhailtag: str = Query("N"),
    limitwarns: str = Query("N"),
    fcster: str = None,
):
    """Babysteps."""
    return cow.handler(
        wfo,
        begints,
        endts,
        phenomena,
        lsrtype,
        hailsize,
        lsrbuffer,
        warningbuffer,
        wind,
        windhailtag,
        limitwarns,
        fcster,
    )


@app.get("/currents")
def currents_service(
    network: str = Query(None),
    networkclass: str = Query(None),
    wfo: str = Query(None, max_length=4),
    state: str = Query(None, max_length=2),
    station: List[str] = Query(None),
    event: str = Query(None),
    minutes: int = Query(1440 * 10),
    fmt: str = Query("json"),
):
    """Babysteps."""
    mediatypes = {
        "json": "application/json",
        "geojson": "application/vnd.geo+json",
        "txt": "text/plain",
    }
    return Response(
        currents.handler(
            network, networkclass, wfo, state, station, event, minutes, fmt
        ),
        media_type=mediatypes[fmt],
    )
