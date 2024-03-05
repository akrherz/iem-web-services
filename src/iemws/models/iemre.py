"""Models for iemre API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class HourlyItem(BaseModel):
    """Data Schema."""

    valid_utc: str = Field(..., title="UTC Timestamp")
    valid_local: str = Field(..., title="Local Station Timestamp")
    skyc_percent: float = Field(..., title="Sky Cloud Coverage [%]")
    air_temp_f: float = Field(..., title="Air Temperature @2m [F]")
    dew_point_f: float = Field(..., title="Air Dew Point @2m [F]")
    uwnd_mps: float = Field(..., title="Wind Speed u-component @10m [ms-1]")
    vwnd_mps: float = Field(..., title="Wind Speed v-component @10m [ms-1]")
    hourly_precip_in: float = Field(..., title="Hourly Precip [inch]")


class HourlySchema(BaseModel):
    """The schema used by this service."""

    data: List[HourlyItem]
