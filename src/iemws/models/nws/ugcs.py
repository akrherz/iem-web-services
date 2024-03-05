"""Models for nws/ugcs API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class UGCItem(BaseModel):
    """Data Schema."""

    ugc: str = Field(..., title="Universal Geographic Code")
    name: str = Field(..., title="Name of Geography")
    state: str = Field(..., title="Two character state abbreviation")
    wfo: str = Field(..., title="WFO identifier associated with the UGC")


class UGCSchema(BaseModel):
    """The schema used by this service."""

    data: List[UGCItem]
