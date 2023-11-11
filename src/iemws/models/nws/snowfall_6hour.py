"""Models for nws/snowfall_6hour API."""
# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    valid: str = Field(..., title="UTC timestamp of observation")
    snow_inch: float = Field(..., title="6 Hour Snowfall (inch)")
    wfo: str = Field(..., title="Weather Forecast Office 3-char ID")
    state: str = Field(..., title="State Field")
    location_id: str = Field(..., title="SHEF station identifier used")
    name: str = Field(..., title="Name of station")
    ugc: str = Field(..., title="IEM computed UGC code associated with report")
    latitude: float = Field(..., title="Latitude of report")
    longitude: float = Field(..., title="Longitude of report")


class Schema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
