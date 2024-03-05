"""Models for idot_dashcam API."""

# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class IDOTDashcamDataItem(BaseModel):
    """Data Schema."""

    index: int = Field(..., title="Unique Key")
    cid: str = Field(..., title="IDOT DashCam Identifier")
    utc_valid: str = Field(..., title="UTC Timestamp")
    lon: float = Field(..., title="Longitude (deg E)")
    lat: float = Field(..., title="Latitude (deg E)")
    imgurl: str = Field(..., title="URI to fetch this webcam image from.")


class IDOTDashcamSchema(BaseModel):
    """The schema used by this service."""

    data: List[IDOTDashcamDataItem]
