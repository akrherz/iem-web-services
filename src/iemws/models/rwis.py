"""Model for rwis API."""

from typing import List

from pydantic import BaseModel, Field


class RWISDataItem(BaseModel):
    """Data Schema."""

    iemid: int = Field(..., title="IEM Identifier")
    tzname: str = Field(..., title="POSIX Timezone of station")
    station: str = Field(..., title="Network assigned station identifier")
    name: str = Field(..., title="Name of the station")
    county: str = Field(..., title="Name of the county the station resides")
    state: str = Field(..., title="State abbreviation of the station")
    network: str = Field(..., title="IEM Network Identifier")
    utc_valid: str = Field(..., title="ISO8601 UTC Timestamp of ob")
    local_valid: str = Field(..., title="ISO8601 Local Timestamp of ob")
    tmpf: float | None = Field(..., title="Air Temperature (F)")
    dwpf: float | None = Field(..., title="Dew Point Temperature (F)")
    relh: float | None = Field(..., title="Relative Humidity (%)")
    vsby: float | None = Field(..., title="Visibility (m)")
    sknt: float | None = Field(..., title="Sustained Wind Speed (kts)")
    drct: float | None = Field(..., title="Wind Direction (degrees)")
    subf: float | None = Field(..., title="Subsurface Temperature (F)")
    gust: float | None = Field(..., title="Wind Gust Speed (kts)")
    tfs0_text: str | None = Field(..., title="Sensor 0 road condition")
    tfs1_text: str | None = Field(..., title="Sensor 1 road condition")
    tfs2_text: str | None = Field(..., title="Sensor 2 road condition")
    tfs3_text: str | None = Field(..., title="Sensor 3 road condition")
    tfs0: float | None = Field(..., title="Sensor 0 temperature (F)")
    tfs1: float | None = Field(..., title="Sensor 1 temperature (F)")
    tfs2: float | None = Field(..., title="Sensor 2 temperature (F)")
    tfs3: float | None = Field(..., title="Sensor 3 temperature (F)")
    pcpn: float | None = Field(..., title="Precipitation (inch)")
    feel: float | None = Field(..., title="Feels Like Temperature (F)")
    lon: float | None = Field(..., title="Longitude (degrees East)")
    lat: float | None = Field(..., title="Latitude (degrees North)")


class RWISSchema(BaseModel):
    """The schema used by this service."""

    data: List[RWISDataItem]
