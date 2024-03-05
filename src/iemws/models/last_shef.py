"""Models for iemre API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class LastSHEFItem(BaseModel):
    """Data Schema."""

    station: str = Field(..., title="Station Identifier")
    utc_valid: str = Field(..., title="UTC Timestamp")
    physical_code: str = Field(..., title="Physical Code")
    duration: str = Field(..., title="Duration")
    source: str = Field(..., title="Source")
    type: str = Field(..., title="Type")
    extremum: str = Field(..., title="Extremum")
    probability: str = Field(..., title="Probability")
    depth: str = Field(..., title="Depth")
    dv_interval: str = Field(..., title="DV Interval")
    qualifer: str = Field(..., title="Qualifier")
    unit_convention: str = Field(..., title="Unit Convention")
    value: float = Field(..., title="Value")
    product_id: str = Field(..., title="NWS Product Source Identifier")


class LastSHEFSchema(BaseModel):
    """The schema used by this service."""

    data: List[LastSHEFItem]
