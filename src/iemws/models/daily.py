"""Models for daily API."""
# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class DailyDataItem(BaseModel):
    """Data Schema."""

    station: str = Field(..., title="Station Idenfifier")
    date: str = Field(..., title="Calendar Date")
    max_tmpf: float = Field(..., title="High Air Temperature [F]")
    min_tmpf: float = Field(..., title="Low Air Temperature [F]")
    precip: float = Field(..., title="Precipitation [inch]")
    max_gust: float = Field(..., title="Maximum Wind Gust [knots]")
    snow: float = Field(..., title="New Snowfall [inch]")
    snowd: float = Field(..., title="Snow Cover Depth[inch]")
    min_rh: float = Field(..., title="Minimum Relative Humidity [%]")
    max_rh: float = Field(..., title="Maximum Relative Humidity [%]")
    min_dwpf: float = Field(..., title="Minimum Dew Point [F]")
    max_dwpf: float = Field(..., title="Maximum Dew Point [F]")
    min_feel: float = Field(..., title="Minimum Feels Like Temperature [F]")
    max_feel: float = Field(..., title="Maximum Feels Like Temperature [F]")
    min_rstage: float = Field(..., title="Minimum Water Stage [ft]")
    max_rstage: float = Field(..., title="Maximum Water Stage [ft]")
    temp_hour: int = Field(..., title="Local Timezone Hour of Day for Ob")
    max_gust_localts: str = Field(..., title="Wind Gust Local Time")
    max_drct: int = Field(..., title="Wind Direction of Max Wind Gust")


class DailySchema(BaseModel):
    """The schema used by this service."""

    data: List[DailyDataItem]
