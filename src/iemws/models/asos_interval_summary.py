"""Models for asos_interval_summary API."""

from typing import List

from pydantic import BaseModel, Field


class AISDataItem(BaseModel):
    """Data Schema."""

    station: str = Field(..., title="Station Identifier")
    max_tmpf: float = Field(..., title="Maximum Air Temperature [F]")
    min_tmpf: float = Field(..., title="Minimum Air Temperature [F]")
    total_precip_in: float = Field(..., title="Total Precipitation [inch]")
    obs_count: int = Field(..., title="Total Observations Considered")
    max_speed_kts: float = Field(..., title="Maximum Wind Speed [kts]")
    max_gust_kts: float = Field(..., title="Maximum Wind Gust [kts]")
    max_speed_drct: float = Field(..., title="Wind Speed Direction [deg]")
    max_gust_drct: float = Field(..., title="Wind Gust Direction [deg]")
    max_speed_time_utc: str = Field(
        ..., title="Time of Maximum Wind Speed [UTC]"
    )
    max_gust_time_utc: str = Field(
        ..., title="Time of Maximum Wind Gust [UTC]"
    )


class AISSchema(BaseModel):
    """The schema used by this service."""

    data: List[AISDataItem]
