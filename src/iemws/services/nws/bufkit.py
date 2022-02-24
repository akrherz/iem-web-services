"""NWS BUFKIT Profiles.

This service exposes a [massive archive](https://mtarchive.geol.iastate.edu)
of [BUFKIT](https://training.weather.gov/wdtd/tools/BUFKIT/index.php) data.
For a single model runtime, this service returns one or more forecast
profiles based on the parameters provided.

There are a number of mechanisms to approach this service.  Here are some
examples requests and what the URL would look like.

- Provide all forecast hours from the most recently available HRRR model for
KDSM.
`/api/1/nws/bufkit.json?model=HRRR&fall=1&station=KDSM`
- Provide closest RAP (assumed) forecast to given location with forecast hour
matching the present time.
`/api/1/nws/bufkit.json?lat=42.5&lon=-92.5`
- Provide the HRRR 11 March 2021 12z run valid at 16z for KDSM.
`/api/1/nws/bufkit.json?model=HRRR&station=KDSM&runtime=2021-03-11T12:00&time=2021-03-11T16:00`
- Provide the closest in time NAM forecast valid at 15z on 10 March 2021 for
KDSM.
`/api/1/nws/bufkit.json?station=KDSM&model=NAM&station=2021-03-10T15:00`

Implementation Notes
--------------------

1. If you set the `fmt` to `txt`, the raw BUFKIT file is returned.
1. You must either provide a `station` or provide a `lat` and `lon` value
to search for the nearest available station for the given model.  The closest
station picked is not an exact science.
1. If you provide a `runtime`, but no valid `time`, this service will use the
current UTC timestamp to locate a forecast hour.  It is always best to
specify both or set `fall=1` to get all forecast hours for that `runtime`.

"""
from datetime import datetime, timedelta, timezone
from io import StringIO
import json
import os

# Third Party
import requests
import pandas as pd
from fastapi import Response, Query, HTTPException, APIRouter
from metpy.units import units
from pyiem.util import utc, logger
from pyiem.nws.bufkit import read_bufkit

# local
from ...models import SupportedFormatsNoGeoJSON
from ...reference import MEDIATYPES

LOG = logger()
router = APIRouter()


def load_stations():
    """Need station details."""
    rows = []
    for name in ["gfs", "hrrr", "nam", "rap", "nam4km"]:
        tablefn = f"/opt/bufkit/bufrgruven/stations/{name}_bufrstations.txt"
        if not os.path.isfile(tablefn):
            continue
        with open(tablefn, encoding="utf-8") as fh:
            for line in fh:
                tokens = line.split()
                if len(tokens) < 5:
                    continue
                rows.append(
                    {
                        "model": name.upper(),
                        "lat": float(tokens[1]),
                        "lon": float(tokens[2]),
                        "sid": tokens[3],
                    }
                )
    return pd.DataFrame(rows)


# Load stations
LOCS = load_stations()
BASEURL = "https://mtarchive.geol.iastate.edu"
ISO9660 = "%Y-%m-%dT%H:%M:%SZ"
# http://www.meteo.psu.edu/bufkit/bufkit_parameters.txt
KGM2 = "kilogram / meter ** 2"
UNITS = {
    "PRES": {"title": "Air Pressure", "unit": "hectopascal"},
    "TMPC": {"title": "Air Temperature", "unit": "degree_Celsius"},
    "TMWC": {"title": "Air Wet Bulb Temperature", "unit": "degree_Celsius"},
    "DWPC": {"title": "Air Dew Point", "unit": "degree_Celsius"},
    "THTE": {"title": "Equivalent Potential Temperature", "unit": "kelvin"},
    "DRCT": {"title": "Wind Direction (from)", "unit": "degree"},
    "SKNT": {"title": "Wind Speed", "unit": "knot"},
    "OMEG": {"title": "Vertical Velocity", "unit": "pascal / second"},
    "CFRL": {"title": "Fractional Cloud Coverage", "unit": "percent"},
    "HGHT": {"title": "Height of Pressure Level", "unit": "meter"},
    "STN": {"title": "Station Identifier", "unit": None},
    "PMSL": {"title": "Mean Sea Level Pressure", "unit": "hectopascal"},
    "SKTC": {"title": "Skin Temperature", "unit": "degree_Celsius"},
    "STC1": {"title": "Layer 1 Soil Temperature", "unit": "kelvin"},
    "SNFL": {"title": "One Hour Accumulated Snowfall", "unit": KGM2},
    "WTNS": {"title": "Soil Moisture Availability", "unit": "percent"},
    "P01M": {"title": "One Hour Total Precipitation", "unit": KGM2},
    "C01M": {"title": "One Hour Convective Precipitation", "unit": KGM2},
    "STC2": {"title": "Layer 2 Soil Temperature", "unit": "kelvin"},
    "LCLD": {"title": "Low Cloud Coverage", "unit": "percent"},
    "MCLD": {"title": "Middle Cloud Coverage", "unit": "percent"},
    "HCLD": {"title": "High Cloud Coverage", "unit": "percent"},
    "SNRA": {
        "title": "Snow Ratio from explicit cloud scheme",
        "unit": "percent",
    },
    "UWND": {"title": "Wind U Component", "unit": "meter / second"},
    "VWND": {"title": "Wind V Component", "unit": "meter / second"},
    "R01M": {"title": "One Hour Surface Runoff", "unit": KGM2},
    "BFGR": {"title": "One Hour Baseflow Runoff", "unit": KGM2},
    "T2MS": {"title": "2m Air Temperature", "unit": "degree_Celsius"},
    "Q2MS": {"title": "2m Specific Humidity", "unit": "kg / kg"},
    "WXTS": {"title": "Snow Precipitation Type", "unit": None},
    "WXTP": {"title": "Ice Pellets Precipitation Type", "unit": None},
    "WXTZ": {"title": "Freezing Rain Precipitation Type", "unit": None},
    "WXTR": {"title": "Rain Precipitation Type", "unit": None},
    "USTM": {"title": "u Component Storm Motion", "unit": "meter / second"},
    "VSTM": {"title": "v Component Storm Motion", "unit": "meter / second"},
    "HLCY": {
        "title": "Storm Relative Helicity",
        "unit": "meter ** 2 / second ** 2",
    },
    "SLLH": {"title": "One Hour Surface Evaporation", "unit": KGM2},
    "WSYM": {"title": "Weather type symbol number", "unit": None},
    "CDBP": {"title": "Pressure at Cloud Base", "unit": "hectopascal"},
    "VSBK": {"title": "Visibility", "unit": "kilometer"},
    "TD2M": {"title": "2m Dew Point", "unit": "degree_Celsius"},
    "STNM": {"title": "Station Number", "unit": None},
    "SLAT": {"title": "Station Latitude", "unit": "degree"},
    "SLON": {"title": "Station Longitude", "unit": "degree"},
    "SELV": {"title": "Station Elevation", "unit": "meter"},
    "SHOW": {"title": "Showalter Index", "unit": "delta_degree_Celsius"},
    "LIFT": {"title": "Lifted Index", "unit": "delta_degree_Celsius"},
    "SWET": {"title": "SWEAT Index", "unit": None},
    "KINX": {"title": "K Index", "unit": "delta_degree_Celsius"},
    "LCLP": {"title": "Pressure at LCL", "unit": "hectopascal"},
    "PWAT": {"title": "Precipitable Water", "unit": KGM2},
    "TOTL": {"title": "Total Totals Index", "unit": "delta_degree_Celsius"},
    "CAPE": {
        "title": "Convective Available Potential Energy",
        "unit": "joule / kilogram",
    },
    "LCLT": {"title": "Temperature at the LCL", "unit": "kelvin"},
    "CINS": {"title": "Convective Inhibition", "unit": "joule / kilogram"},
    "EQLV": {"title": "Equilibrium level", "unit": "hectopascal"},
    "LFCT": {"title": "Level of Free Convection", "unit": "hectopascal"},
    "BRCH": {"title": "Bulk Richardson Number", "unit": None},
}


def do_gr(ctx):
    """Custom schema."""
    s = ctx["sndf"]["STIM PRES HGHT TMPC DWPC DRCT SKNT OMEG".split()]
    levels = s[s["STIM"] == ctx["fhour"]].drop("STIM", axis=1)
    levels = levels.rename(
        {
            "PRES": "pressure",
            "HGHT": "height",
            "TMPC": "temperature",
            "DWPC": "dewpoint",
            "DRCT": "wind_from",
            "SKNT": "wind_speed",
            "OMEG": "uvv",
        },
        axis=1,
    )
    levels["wind_speed"] = (
        (levels["wind_speed"].values * units("knot"))
        .to(units("meter / second"))
        .m
    )
    levels["wind_speed"] = levels["wind_speed"].round(2)
    records = levels.to_dict(orient="records")
    res = {
        "time": ctx["valid"].strftime(ISO9660),
        "lat": ctx["lat"],
        "lon": ctx["lon"],
        "source": {
            "type": "model",
            "model": ctx["model"],
            "run_time": ctx["runtime"].strftime(ISO9660),
            "forecast_hour": ctx["fhour"],
        },
        "levels": records,
        "units": {
            "pressure": ["MB", "millibars", "hPa", "hectopascals"],
            "height": ["M", "meters"],
            "temperature": ["C", "celsius"],
            "dewpoint": ["C", "celsius"],
            "wind_from": ["DEG", "degrees"],
            "wind_speed": ["MPS", "meters per second"],
            "uvv": ["UBS", "microbars per second"],
        },
    }
    return res


def handler(ctx):
    """Handle the request, return dict"""
    begin = utc()
    model = ctx["model"].upper()
    # compute stations to attempt requests
    station = ctx["station"]
    stations = [
        station,
    ]
    if ctx["station"] is None:
        lon = ctx["lon"]
        lat = ctx["lat"]
        if lat is None or lon is None:
            raise HTTPException(500, detail="Need to provide lat/lon")
        df = LOCS[LOCS["model"] == model]
        dist = ((df["lat"] - lat) ** 2 + (df["lon"] - lon) ** 2) ** 0.5
        idxs = dist.sort_values(ascending=True).index.values[:10]
        stations = df.loc[idxs]["sid"].values
    else:
        row = LOCS[(LOCS["model"] == model) & (LOCS["sid"] == station)]
        if row.empty:
            raise HTTPException(
                500,
                detail=f"Unknown station '{station}' for model '{model}'",
            )
        lat = float(row["lat"])
        lon = float(row["lon"])

    # compute runtimes to attempt to request model data for
    runtimes = [
        ctx["runtime"],
    ]
    valid = utc()
    valid = utc(valid.year, valid.month, valid.day, valid.hour)
    if ctx["time"] is not None:
        valid = ctx["time"].replace(tzinfo=timezone.utc)
    if ctx["runtime"] is None:
        if model in ["HRRR", "RAP"]:
            hr1 = timedelta(hours=1)
            runtimes = [valid, valid - hr1, valid - hr1 * 2, valid - hr1 * 3]
        else:
            hr6 = timedelta(hours=6)
            runtime = valid - timedelta(hours=(valid.hour % 6))
            runtimes = [runtime, runtime - hr6, runtime - hr6 * 2]

    sio = StringIO()
    sz = 0
    for (station, runtime) in [(x, y) for x in stations for y in runtimes]:
        runtime = runtime.replace(tzinfo=timezone.utc)
        prefix = model.lower()
        if (
            model
            in [
                "NAM",
            ]
            and runtime.hour in [6, 18]
        ):
            prefix = "namm"
        if model == "GFS":
            prefix = "gfs3"
        url = runtime.strftime(
            f"{BASEURL}/%Y/%m/%d/bufkit/%H/{model.lower()}/{prefix}_"
            f"{station.lower()}.buf"
        )
        try:
            req = requests.get(url, timeout=20)
        except Exception as exp:
            LOG.info("URL %s failed with %s", url, exp)
            raise HTTPException(
                503,
                detail="mtarchive backend failed, try later please.",
            ) from exp
        if req.status_code == 200:
            sz = sio.write(req.text)
            row = LOCS[(LOCS["model"] == model) & (LOCS["sid"] == station)]
            lat = float(row["lat"])
            lon = float(row["lon"])
            break
    if sz == 0:
        raise HTTPException(500, detail="Could not find any profile.")
    if ctx["fmt"] == "txt":
        return sio.getvalue()

    sndf, stndf = read_bufkit(sio)
    fhour = int((valid - runtime).total_seconds() / 3600)
    fhours = [fhour]
    if ctx["gr"]:
        res = do_gr(vars())
        return json.dumps(res)
    stndf = stndf.drop("utc_valid", axis=1)
    res = {
        "server_time": utc().strftime(ISO9660),
        "variables": UNITS,
        "profiles": [],
        "lat": lat,
        "lon": lon,
        "source": {
            "type": "model",
            "model": model,
            "station": station,
            "run_time": runtime.strftime(ISO9660),
        },
    }
    if ctx["fall"]:
        fhours = stndf.index.values
    for fhour in fhours:
        if fhour not in sndf.index:
            raise HTTPException(500, f"Failed to find forecast hour {fhour}")
        levels = sndf[sndf["STIM"] == fhour].drop("STIM", axis=1)
        res["profiles"].append(
            {
                "forecast_hour": int(fhour),
                "time": (runtime + timedelta(hours=fhour)).strftime(ISO9660),
                "levels": levels.to_dict(orient="records"),
                "parameters": stndf.loc[fhour].to_dict(),
            }
        )

    res["generation_time_seconds"] = (utc() - begin).total_seconds()
    return json.dumps(res).replace("NaN", "null")


@router.get("/nws/bufkit.{fmt}", description=__doc__, tags=["nws"])
def service(
    fmt: SupportedFormatsNoGeoJSON,
    lon: float = Query(None, min=-180, max=180, description="degrees E"),
    lat: float = Query(None, min=-90, max=90, description="degrees N"),
    model: str = Query(
        "RAP",
        description="Model in 'GFS', 'HRRR', 'NAM', 'NAM4KM', 'RAP'",
        max_length=6,
    ),
    time: datetime = Query(None, description="Profile Valid Time in UTC"),
    runtime: datetime = Query(None, description="Model Init Time UTC"),
    station: str = Query(None, description="bufkit site identifier"),
    fall: bool = Query(False, description="Include all forecast hours"),
    gr: bool = Query(False, description="Use Gibson Ridge JSON Schema"),
):
    """Replaced above."""
    if model not in ["GFS", "HRRR", "NAM", "NAM4KM", "RAP"]:
        raise HTTPException(500, "Invalid model parameter provided.")
    ctx = {
        "fmt": fmt,
        "lon": lon,
        "lat": lat,
        "model": model,
        "time": time,
        "runtime": runtime,
        "station": station,
        "fall": fall,
        "gr": gr,
    }
    return Response(handler(ctx), media_type=MEDIATYPES[fmt])


service.__doc__ = __doc__
