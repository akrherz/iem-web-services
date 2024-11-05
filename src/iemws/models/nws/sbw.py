"""Models for various SBW APIs."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    wfo: str = Field(..., title="Weather Forecast Office 3-char ID")
    vtec_year: int = Field(..., title="VTEC Year")
    phenomena: str = Field(..., title="VTEC Phenomena")
    significance: str = Field(..., title="VTEC Significance")
    eventid: int = Field(..., title="VTEC Event ID")
    uri: str = Field(..., title="URI to more information")
    windtag: float = Field(..., title="Wind Tag")
    hailtag: float = Field(..., title="Hail Tag")
    tornadotag: str = Field(..., title="Tornado Tag")
    damagetag: str = Field(..., title="Damage Tag")
    is_emergency: bool = Field(..., title="Is Emergency")
    is_pds: bool = Field(..., title="Is Particularly Dangerous Situation")
    windthreat: float = Field(..., title="Wind Threat")
    hailthreat: float = Field(..., title="Hail Threat")
    squalltag: str = Field(..., title="Squall Tag")
    product_id: str = Field(..., title="Product ID")
    product_signature: str = Field(..., title="Product Signature")
    issue: str = Field(..., title="Issue Time in UTC")
    expire: str = Field(..., title="Expire Time in UTC")
    polygon_begin: str = Field(..., title="Polygon Begin Time in UTC")
    polygon_end: str = Field(..., title="Polygon End Time in UTC")


class StormBasedWarningSchema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
