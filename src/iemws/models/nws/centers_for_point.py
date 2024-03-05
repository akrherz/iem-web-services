"""Models for nws/centers_for_point API."""

# pylint: disable=no-name-in-module,too-few-public-methods
from typing import List

from pydantic import BaseModel, Field


class Item(BaseModel):
    """Data Schema."""

    wfo: str = Field(..., title="Weather Forecast Office")
    rfc: str = Field(..., title="River Forecast Center")
    cwsu: str = Field(..., title="Center Weather Service Unit")


class Schema(BaseModel):
    """The schema used by this service."""

    data: List[Item]
