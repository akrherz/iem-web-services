"""Models for idot_rwiscam API."""
# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class IDOTRWIScamDataItem(BaseModel):
    """Data Schema."""

    index: int = Field(..., title="Unique Key")
    cid: str = Field(..., title="IDOT RWIS Identifier")
    utc_valid: str = Field(..., title="UTC Timestamp")
    lon: float = Field(..., title="Longitude (deg E)")
    lat: float = Field(..., title="Latitude (deg E)")
    imgurl0: str = Field(..., title="URI for RWIS View 0.")
    imgurl1: str = Field(..., title="URI for RWIS View 1.")
    imgurl2: str = Field(..., title="URI for RWIS View 2.")
    imgurl3: str = Field(..., title="URI for RWIS View 3.")
    imgurl4: str = Field(..., title="URI for RWIS View 4.")
    imgurl5: str = Field(..., title="URI for RWIS View 5.")
    imgurl6: str = Field(..., title="URI for RWIS View 6.")
    imgurl7: str = Field(..., title="URI for RWIS View 7.")
    imgurl8: str = Field(..., title="URI for RWIS View 8.")
    imgurl9: str = Field(..., title="URI for RWIS View 9.")


class IDOTRWIScamSchema(BaseModel):
    """The schema used by this service."""

    data: List[IDOTRWIScamDataItem]
