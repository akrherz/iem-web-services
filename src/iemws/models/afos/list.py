"""Models for iemre API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class AFOSListItem(BaseModel):
    """Data Schema."""

    entered: str = Field(..., title="WMO Text Product Issuance Time (UTC)")
    link: str = Field(
        ...,
        title="URL to the IEM webpage showing this product.",
    )
    text_link: str = Field(
        ...,
        title="URL to the raw text of the product.",
    )
    pil: str = Field(
        ...,
        title="3-6 letter PIL with trailing whitespace trimmed.",
    )
    product: str = Field(..., title="IEM Text Product ID.")
    cccc: str = Field(..., title="Four character WMO source.")
    count: int = Field(..., title="Number of text products at this identifier")


class AFOSListSchema(BaseModel):
    """The schema used by this service."""

    data: List[AFOSListItem]
