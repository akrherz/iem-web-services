"""Models for asos_interval_summary API."""

# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class AISDataItem(BaseModel):
    """Data Schema."""

    station: str = Field(..., title="Station Identifier")
    max_tmpf: float = Field(..., title="Maximum Air Temperature [F]")
    min_tmpf: float = Field(..., title="Minimum Air Temperature [F]")
    total_precip_in: float = Field(..., title="Total Precipitation [inch]")
    obs_count: int = Field(..., title="Total Observations Considered")


class AISSchema(BaseModel):
    """The schema used by this service."""

    data: List[AISDataItem]
