"""Models for currents API."""

# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class ObHistoryDataItem(BaseModel):
    """Data Schema."""

    utc_valid: str = Field(..., title="UTC Timestamp")
    local_valid: str = Field(..., title="Local Station Timestamp")
    tmpf: float = Field(..., title="Air Temperature [F]")
    dwpf: float = Field(..., title="Dew Point Temperature [F]")
    relh: float = Field(..., title="Relative Humidity [%]")
    feel: float = Field(..., title="Feels Like Temperature [F]")
    sknt: float = Field(..., title="Wind Speed [kts]")
    gust: float = Field(..., title="Wind Gust [kts]")
    drct: float = Field(..., title="Wind Direction [deg]")
    vsby: float = Field(..., title="Visibility [miles]")
    skyc1: str = Field(..., title="Cloud Coverage Code Level 1")
    skyl1: str = Field(..., title="Cloud Base Level 1 [ft]")
    skyc2: str = Field(..., title="Cloud Coverage Code Level 2")
    skyl2: str = Field(..., title="Cloud Base Level 2 [ft]")
    skyc3: str = Field(..., title="Cloud Coverage Code Level 3")
    skyl3: str = Field(..., title="Cloud Base Level 3 [ft]")
    skyc4: str = Field(..., title="Cloud Coverage Code Level 4")
    skyl4: str = Field(..., title="Cloud Base Level 4 [ft]")
    alti: float = Field(..., title="Altimeter [inch]")
    mslp: float = Field(..., title="Sea Level Pressure [mb]")
    p01i: float = Field(..., title="ASOS 60 Minute Precipitation Accum [inch]")
    phour: float = Field(..., title="Precip since top of the hour [inch]")
    max_tmpf_6hr: float = Field(..., title="ASOS 6 Hour Max Temperature [F]")
    min_tmpf_6hr: float = Field(..., title="ASOS 6 Hour Min Temperature [F]")
    p03i: float = Field(..., title="ASOS 3 Hour Precipitation Accum [inch]")
    p06i: float = Field(..., title="ASOS 6 Hour Precipitation Accum [inch]")
    p24i: float = Field(..., title="ASOS 24 Hour Precipitation Accum [inch]")
    raw: str = Field(..., title="METAR or SHEF information")
    max_tmpf_6hr: float = Field(..., title="METAR 6 Hour Max Temp [F]")
    min_tmpf_6hr: float = Field(..., title="METAR 6 Hour Min Temp [F]")
    wxcodes: str = Field(..., title="Present Weather METAR Codes")
    snowdepth: str = Field(..., title="Snow Depth [inch]")


class ObHistorySchema(BaseModel):
    """The schema used by this service."""

    data: List[ObHistoryDataItem]
