"""Models for nws/lsrs_by_point API."""
# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    valid: str = Field(..., title="UTC timestamp begining the outlook period")
    magnitude: float = Field(..., title="Magnitude, when available")
    city: str = Field(..., title="City Field")
    county: str = Field(..., title="County Field")
    state: str = Field(..., title="State Field")
    source: str = Field(..., title="Source Field")
    remark: str = Field(..., title="Free-form Remark Field")
    wfo: str = Field(..., title="Issuance Weather Forecast Office 3-char ID")
    typetext: str = Field(..., title="Type")
    product_id: str = Field(..., title="IEM identifier for NWS Text Product")
    unit: str = Field(..., title="Magnitude Units")
    qualifier: str = Field(
        ..., title="Qualifier Field (Measured, Estimated, Unknown)"
    )
    ugc: str = Field(..., title="IEM computed UGC code associated with report")
    product_id_summary: str = Field(
        ...,
        title="IEM identifier for NWS Text Product that was a LSR Summary.",
    )


class Schema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
