"""Model for nws/emergencies API."""
# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    year: int = Field(..., title="Year of the Event.")
    wfo: str = Field(
        ..., title="Three character NWS Weather Forecast Office (WFO)"
    )
    eventid: int = Field(..., title="VTEC Event Identifier")
    phenomena: str = Field(..., title="VTEC Phenomena Code")
    significance: str = Field(..., title="VTEC Significance Code")
    utc_product_issue: str = Field(..., title="UTC Timestamp of Product Issue")
    utc_init_expire: str = Field(..., title="UTC Initial Event Expiration")
    utc_issue: str = Field(..., title="UTC Event Begin Timestamp")
    utc_expire: str = Field(..., title="UTC Event End Timestamp")
    states: str = Field(..., title="Comma seperated state abbreviations")
    uri: str = Field(..., title="IEM website URI for the event.")
    is_sbw: bool = Field(..., title="For GeoJSON, is the polygon storm based.")


class Schema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
