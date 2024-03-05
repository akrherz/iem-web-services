"""Models for nws/outlook_by_point API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    day: int = Field(..., title="Outlook Day Number")
    outlook_type: str = Field(
        ..., title="Outlook Type Convective Fire Excessive rainfall"
    )
    product_issue: str = Field(..., title="UTC timestamp of outlook issuance")
    issue: str = Field(..., title="UTC timestamp begining the outlook period")
    expire: str = Field(..., title="UTC timestamp ending the outlook period")
    threshold: str = Field(..., title="Outlook threshold identifier")
    category: str = Field(..., title="Outlook category (Hail, Tornado, etc)")


class Schema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
