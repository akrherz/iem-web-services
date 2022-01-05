"""Models for county_zone API."""
# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class CountyZoneDataItem(BaseModel):
    """Data Schema."""

    utc_product_issue: str = Field(..., title="UTC Timestamp of Text Product")
    utc_issue: str = Field(..., title="UTC Timestamp of Issue")
    utc_expire: str = Field(..., title="UTC Timestamp of Expire")
    ph_sig: str = Field(
        ...,
        title="Convienece combined phenomena and significance",
    )
    wfo: str = Field(..., title="Three-letter WFO/CWA Identifier")
    eventid: int = Field(..., title="VTEC Event ID")
    phenomena: str = Field(..., title="VTEC Phenomena Identifier")
    significance: str = Field(..., title="VTEC Significance Identifier")
    ugc: str = Field(..., title="Six-Character UGC Code")
    nws_color: str = Field(..., title="NWS HEX Color Code with hash prefix")
    event_label: str = Field(..., title="English Name of Event")


class CountyZoneSchema(BaseModel):
    """The schema used by this service."""

    data: List[CountyZoneDataItem]
