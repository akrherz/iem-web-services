"""Models for climodat API."""

# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class PORDailyClimoDataItem(BaseModel):
    """Data Schema."""

    station: str = Field(..., title="Station Idenfifier")
    date: str = Field(..., title="Calendar Date")
    years: int = Field(..., title="Number of Years of Data")
    high_min: int = Field(..., title="Maximum High Temperature [F]")
    high_min_years: str = Field(..., title="Years of Minimum High Temperature")
    high_avg: float = Field(..., title="Average High Temperature [F]")
    high_max: int = Field(..., title="Maximum High Temperature [F]")
    high_max_years: str = Field(..., title="Years of Maximum High Temperature")
    low_min: int = Field(..., title="Minimum Low Temperature [F]")
    low_min_years: str = Field(..., title="Years of Minimum Low Temperature")
    low_avg: float = Field(..., title="Average Low Temperature [F]")
    low_max: int = Field(..., title="Maximum Low Temperature [F]")
    low_max_years: str = Field(..., title="Years of Maximum Low Temperature")
    precip: float = Field(..., title="Average Precipitation [inch]")
    precip_max: float = Field(..., title="Maximum Precipitation [inch]")
    precip_max_years: str = Field(..., title="Years of Maximum Precipitation")


class PORDailyClimoSchema(BaseModel):
    """The schema used by this service."""

    data: List[PORDailyClimoDataItem]
