"""Models for network API."""

# pylint: disable=no-name-in-module
from typing import List

from pydantic import BaseModel, Field


class NetworkDataItem(BaseModel):
    """Data Schema."""

    index: int = Field(..., title="format index value, no meaning")
    id: str = Field(..., title="station identifier")
    synop: str = Field(None, title="synop identifier")
    name: str = Field(..., title="station name")
    state: str = Field(..., title="state abbreviation")
    country: str = Field(..., title="country abbreviation")
    elevation: float = Field(..., title="elevation in meters")
    network: str = Field(..., title="network identifier")
    online: bool = Field(..., title="is station online")
    params: str = Field(..., title="comma separated list of parameters")
    county: str = Field(..., title="county name")
    plot_name: str = Field(..., title="Sometimes shorter station name")
    climate_site: str = Field(
        ...,
        title=(
            "IEM long term climate site used for this station's climatology"
        ),
    )
    remote_id: int = Field(
        None, title="remote identifier used by some networks"
    )
    nwn_id: str = Field(None, title="Unused field")
    spri: str = Field(None, title="Unused field")
    wfo: str = Field(..., title="NWS WFO identifier covering this station")
    archive_begin: str = Field(..., title="UTC timestamp of when data begins")
    archive_end: str = Field(None, title="UTC timestamp of when data ends")
    modified: str = Field(..., title="UTC timestamp of last modification")
    tzname: str = Field(..., title="timezone name")
    iemid: int = Field(..., title="IEM internal identifier")
    metasite: bool = Field(..., title="Is this a metasite, no obs")
    sigstage_low: str = Field(None, title="Low stage flood level")
    sigstage_action: str = Field(None, title="Action stage flood level")
    sigstage_bankfull: str = Field(None, title="Bankfull stage flood level")
    sigstage_flood: str = Field(None, title="Flood stage flood level")
    sigstage_moderate: str = Field(None, title="Moderate stage flood level")
    sigstage_major: str = Field(None, title="Major stage flood level")
    sigstage_record: str = Field(None, title="Record stage flood level")
    ugc_county: str = Field(None, title="NWS UGC county identifier")
    ugc_zone: str = Field(None, title="NWS UGC zone identifier")
    ncdc81: str = Field(None, title="NCDC 1981-2010 climate site identifier")
    temp24_hour: str = Field(None, title="For daily sites, hour of reports")
    precip24_hour: str = Field(None, title="For daily sites, hour of reports")
    ncei91: str = Field(None, title="NCEI 1991-2020 climate site identifier")
    wigos: str = Field(None, title="WIGOS identifier")
    longitude: float = Field(..., title="station longitude, degrees East")
    latitude: float = Field(..., title="station latitude, degrees North")


class NetworkSchema(BaseModel):
    """The schema used by this service."""

    data: List[NetworkDataItem]
