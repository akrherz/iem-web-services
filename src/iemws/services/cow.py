"""IEM Cow (NWS Storm Based Warning Verification) API

See [IEM Cow](https://mesonet.agron.iastate.edu/cow/) webpage for the user
frontend to this API and for more discussion about what this does.

While this service only emits JSON, the JSON response embeds two GeoJSON
objects providing the storm reports and warnings.

Updated **2025-04-11**, added `enable_shared_border` parameter to enable
shared border calculation. This is a boolean parameter that defaults to
False when more than one (year * wfos) of data is requested.
It is recommended to only enable this parameter when needed.
"""

import json
from datetime import datetime
from typing import List, Optional

import geopandas as gpd
import pandas as pd
from fastapi import APIRouter, Query
from pyiem.database import sql_helper
from pyiem.reference import ISO8601
from pyiem.util import utc
from shapely.ops import unary_union

from ..util import get_sqlalchemy_conn

LSRTYPE2PHENOM = {
    "T": "TO",
    "H": "SV",
    "G": "SV",
    "D": "SV",
    "F": "FF",
    "x": "FF",
    "M": "MA",
    "W": "MA",
    "h": "MA",
    "2": "DS",
    "q": "SQ",
}
router = APIRouter()
TOR_IN_SVRTORPOSSIBLE = "tor_in_svrtorpossible"


class COWSession:
    """Things that we could do while generating Cow stats"""

    def __init__(
        self,
        wfo,
        begints: datetime,
        endts: datetime,
        phenomena,
        lsrtype,
        hailsize,
        lsrbuffer,
        warningbuffer,
        wind,
        windhailtag: bool,
        limitwarns: bool,
        fcster,
        enable_shared_border: bool,
    ):
        """Build out our session based on provided fields"""
        self.wfo = [x[:4] for x in wfo]
        # Figure out the begin and end times
        self.begints = begints
        self.endts = endts
        # Storage of data
        self.events: pd.DataFrame = gpd.GeoDataFrame()
        self.events_buffered = None
        self.stormreports: pd.DataFrame = gpd.GeoDataFrame()
        self.stormreports_buffered = None
        self.stats = {}
        # query parameters
        self.phenomena = [x[:2] for x in phenomena]
        if not self.phenomena:
            self.phenomena = ["TO", "SV", "FF", "MA", "DS", "SQ"]
        self.lsrtype = [x[:2] for x in lsrtype]
        if not self.lsrtype:
            self.lsrtype = ["TO", "SV", "FF", "MA", "DS", "SQ"]
        self.hailsize = hailsize
        self.lsrbuffer = lsrbuffer
        self.warningbuffer = warningbuffer
        self.wind = wind
        self.windhailtag = windhailtag
        self.limitwarns = limitwarns
        self.fcster = fcster
        self.enable_shared_border = enable_shared_border

    def milk(self):
        """Milk the Cow and see what happens"""
        with get_sqlalchemy_conn("postgis") as dbconn:
            self.load_events(dbconn)
            self.load_stormreports(dbconn)
            if self.enable_shared_border:
                self.compute_shared_border(dbconn)
        self.sbw_verify()
        self.area_verify()
        self.compute_stats()

    def compute_stats(self):
        """Fill out the stats attribute"""
        _ev = self.events
        _sr: pd.DataFrame = self.stormreports
        self.stats["area_verify[%]"] = (
            0
            if _ev.empty
            else _ev["areaverify"].sum() / _ev["parea"].sum() * 100.0
        )
        self.stats["shared_border[%]"] = (
            0
            if _ev.empty
            else _ev["sharedborder"].sum() / _ev["perimeter"].sum() * 100.0
        )
        self.stats["max_leadtime[min]"] = (
            None if _sr.empty else _sr["leadtime"].max()
        )
        self.stats["min_leadtime[min]"] = (
            None if _sr.empty else _sr["leadtime"].min()
        )
        self.stats["avg_leadtime[min]"] = (
            None if _sr.empty else _sr["leadtime"].mean()
        )
        self.stats["avg_leadtime_firstreport[min]"] = (
            None if _ev.empty else _ev["lead0"].mean()
        )
        self.stats["tdq_stormreports"] = (
            0 if _sr.empty else len(_sr[_sr["tdq"]].index)
        )
        self.stats["unwarned_reports"] = (
            0 if _sr.empty else len(_sr[~_sr["warned"] & ~_sr["tdq"]].index)
        )
        self.stats["warned_reports"] = (
            0 if _sr.empty else len(_sr[_sr["warned"]].index)
        )
        self.stats["events_verified"] = (
            0 if _ev.empty else len(_ev[_ev["verify"]].index)
        )
        self.stats["events_total"] = len(_ev.index)
        self.stats["reports_total"] = len(_sr.index)
        if self.stats["reports_total"] > 0:
            pod = self.stats["warned_reports"] / float(
                self.stats["reports_total"]
            )
        else:
            pod = 0
        self.stats["POD[1]"] = pod
        if self.stats["events_total"] > 0:
            far = (
                self.stats["events_total"] - self.stats["events_verified"]
            ) / self.stats["events_total"]
        else:
            far = 0
        self.stats["FAR[1]"] = far
        if pod > 0:
            self.stats["CSI[1]"] = (((pod) ** -1 + (1 - far) ** -1) - 1) ** -1
        else:
            self.stats["CSI[1]"] = 0.0
        self.stats["avg_size[sq km]"] = 0 if _ev.empty else _ev["parea"].mean()
        self.stats["size_poly_vs_county[%]"] = (
            0 if _ev.empty else _ev["parea"].sum() / _ev["carea"].sum() * 100.0
        )
        self.stats["svr_with_torpossible_total"] = int(
            _ev["svr_tornado_possible"].sum()
        )
        self.stats["svr_with_torpossible_verified"] = int(
            _ev[TOR_IN_SVRTORPOSSIBLE].sum()
        )
        # Prevent NaN values from above
        for key, stat in self.stats.items():
            if pd.isnull(stat):
                self.stats[key] = None

    def lsr_limiter_list(self) -> list:
        """How to limit LSR types"""
        # This adds in some extra things that the database will ignore
        ltypes = self.lsrtype.copy()
        # Handle aliases
        if "TO" in self.lsrtype:
            ltypes.append("T")
        if "SV" in self.lsrtype:
            ltypes.extend(["H", "G", "D"])
        if "FF" in self.lsrtype:
            ltypes.extend(["F", "x"])
        if "MA" in self.lsrtype:
            ltypes.extend(["M", "W", "h"])
        if "DS" in self.lsrtype:
            ltypes.append("2")
        if "SQ" in self.lsrtype:
            ltypes.append("q")
        return ltypes

    def sql_fcster_limiter(self):
        """Should we limit the fcster column?"""
        return " " if self.fcster is None else " and fcster ILIKE :fcster "

    def sql_tag_limiter(self):
        """Do we need to limit the events based on tags"""
        if not self.limitwarns:
            return " "
        return (
            " and ((w.windtag >= :wind or "
            "w.hailtag >= :hailsize) or "
            " (w.windtag is null and w.hailtag is null)) "
        )

    def load_events(self, dbconn):
        """Build out the listing of events based on the request"""
        wlimit = "" if not self.wfo else " and w.wfo = ANY(:wfos)"
        self.events = gpd.read_postgis(
            sql_helper(
                """
        WITH stormbased as (
            SELECT wfo, phenomena, eventid, hailtag, windtag,
            coalesce(tornadotag, '') = 'POSSIBLE' as svr_tornado_possible,
            geom, significance,
            ST_area(ST_transform(geom,2163)) / 1000000.0 as parea,
            ST_perimeter(ST_transform(geom,2163)) as perimeter,
            ST_xmax(geom) as lon0, ST_ymax(geom) as lat0,
            extract(year from issue at time zone 'UTC') as year,
            product_id
            from sbw w WHERE status = 'NEW' {wlimit}
            and issue >= :begints and issue < :endts and expire < :endts
            and significance = 'W'
            and phenomena = ANY(:phenomena) {taglimiter}
        ),
        countybased as (
            SELECT w.wfo, phenomena, eventid, significance,
            max(w.status) as statuses,
            array_agg(u.ugc) as ar_ugc,
            array_agg(u.name ||' '||u.state) as ar_ugcname,
            sum(ST_area(ST_transform(u.geom,2163)) / 1000000.0) as carea,
            min(issue at time zone 'UTC') as missue,
            max(expire at time zone 'UTC') as mexpire,
            extract(year from issue at time zone 'UTC') as year, w.fcster
            from warnings w JOIN ugcs u on (u.gid = w.gid) WHERE
            w.gid is not null {wlimit} and
            issue >= :begints and issue < :endts and expire < :endts
            and significance = 'W'
            and phenomena = ANY(:phenomena)
            {flimiter}
            GROUP by w.wfo, phenomena, eventid, significance, year, fcster
        )
        SELECT s.year::int, s.wfo, s.phenomena, s.eventid, s.geom,
        c.missue as issue,
        c.mexpire as expire, c.statuses, c.fcster, svr_tornado_possible,
        s.significance, s.hailtag, s.windtag, c.carea, c.ar_ugc,
        s.lat0, s.lon0, s.perimeter, s.parea, c.ar_ugcname,
        s.year || s.wfo || s.eventid || s.phenomena || s.significance ||
        row_number() OVER (PARTITION by s.year, s.wfo, s.eventid, s.phenomena,
        s.significance ORDER by c.missue ASC) as key, s.product_id
        from stormbased s JOIN countybased c on
        (c.eventid = s.eventid and c.wfo = s.wfo and c.year = s.year
        and c.phenomena = s.phenomena and c.significance = s.significance)
        ORDER by issue ASC
        """,
                wlimit=wlimit,
                taglimiter=self.sql_tag_limiter(),
                flimiter=self.sql_fcster_limiter(),
            ),
            dbconn,
            params={
                "begints": self.begints,
                "endts": self.endts,
                "phenomena": self.phenomena,
                "wfos": self.wfo,
                "fcster": self.fcster,
                "wind": self.wind,
                "hailsize": self.hailsize,
            },
            crs=4326,
            index_col="key",
        )  # type: ignore
        self.events = self.events.assign(
            status=lambda df_: df_["statuses"],  # le sigh
            stormreports=lambda df_: [[] for _ in range(len(df_.index))],
            stormreports_all=lambda df_: [[] for _ in range(len(df_.index))],
        )
        self.events["verify"] = False
        self.events[TOR_IN_SVRTORPOSSIBLE] = False
        self.events["lead0"] = None
        self.events["areaverify"] = 0.0
        self.events["sharedborder"] = 0.0
        if self.events.empty:
            return
        self.events["visual_imgurl"] = (
            "https://mesonet.agron.iastate.edu/GIS/radmap.php?visual=1&vtec="
            + self.events["year"].astype(str)
            + ".K"
            + self.events["wfo"]
            + "."
            + self.events["phenomena"]
            + "."
            + self.events["significance"]
            + "."
            + self.events["eventid"].astype(str).str.pad(4, fillchar="0")
            + "&lsrbuffer="
            + str(self.lsrbuffer)
        )
        self.events["product_text"] = (
            "https://mesonet.agron.iastate.edu/api/1/nwstext/"
            + self.events["product_id"]
        )
        self.events["product_href"] = (
            "https://mesonet.agron.iastate.edu/p.php?pid="
            + self.events["product_id"]
        )
        self.events["link"] = (
            "https://mesonet.agron.iastate.edu/vtec/f/"
            + self.events["year"].astype(str)
            + "-O-NEW-K"
            + self.events["wfo"]
            + "-"
            + self.events["phenomena"]
            + "-"
            + self.events["significance"]
            + "-"
            + self.events["eventid"].astype(str).str.pad(4, fillchar="0")
        )
        s2163 = self.events["geom"].to_crs(epsg=2163)
        self.events_buffered = s2163.buffer(self.warningbuffer * 1000.0)

    def load_stormreports(self, dbconn):
        """Build out the listing of storm reports based on the request"""
        params = {
            "begints": self.begints,
            "ltypes": self.lsr_limiter_list(),
            "endts": self.endts,
            "hailsize": self.hailsize,
            "wind": self.wind,
            "wfos": self.wfo,
        }
        wlimit = "" if not self.wfo else " and wfo = ANY(:wfos)"
        self.stormreports = gpd.read_postgis(
            sql_helper(
                """
        SELECT distinct valid at time zone 'UTC' as valid,
        type, magnitude, city, county, state,
        source, remark, wfo, typetext, ST_x(geom) as lon0, ST_y(geom) as lat0,
        geom
        from lsrs w WHERE valid >= :begints and valid <= :endts
        {wlimit} and type = ANY(:ltypes)
        and ((type = 'M' and magnitude >= 34) or type = '2' or
        (type in ('H', 'h') and magnitude >= :hailsize) or type = 'W' or
         type = 'T' or (type = 'G' and magnitude >= :wind) or
         type in ('D', 'F', 'x', 'q')) ORDER by valid ASC
        """,
                wlimit=wlimit,
            ),
            dbconn,
            params=params,
            geom_col="geom",
            crs=4326,
        )  # type: ignore
        self.stormreports["events"] = [
            [] for _ in range(len(self.stormreports.index))
        ]
        self.stormreports["tdq"] = False
        self.stormreports["warned"] = False
        self.stormreports["leadtime"] = None
        self.stormreports["lsrtype"] = self.stormreports["type"].map(
            LSRTYPE2PHENOM
        )
        if self.stormreports.empty:
            return
        self.stormreports["link"] = (
            "https://mesonet.agron.iastate.edu/lsr/#"
            + self.stormreports["wfo"]
            + "/"
            + self.stormreports["valid"].dt.strftime("%Y%m%d%H%M")
            + "/"
            + self.stormreports["valid"].dt.strftime("%Y%m%d%H%M")
        )
        s2163 = self.stormreports["geom"].to_crs(epsg=2163)
        self.stormreports_buffered = s2163.buffer(self.lsrbuffer * 1000.0)

    def compute_shared_border(self, dbconn):
        """Compute a stat"""
        # re ST_Buffer(simple_geom) see akrherz/iem#163
        wlimit = "" if not self.wfo else " and w.wfo = ANY(:wfos) "
        df = pd.read_sql(
            sql_helper(
                """
            WITH stormbased as (
                SELECT geom, wfo, eventid, phenomena, significance,
                extract(year from issue at time zone 'UTC') as year
                from sbw w WHERE status = 'NEW' {wlimit}
                and issue >= :begints and issue < :endts and expire < :endts
                and significance = 'W'
                and phenomena = ANY(:phenomena) {taglimiter}),
            countybased as (
                SELECT ST_Union(ST_Buffer(u.simple_geom, 0)) as geom,
                w.wfo, phenomena, eventid, significance,
                extract(year from issue at time zone 'UTC') as year, w.fcster
                from warnings w JOIN ugcs u on (u.gid = w.gid) WHERE
                w.gid is not null {wlimit} and
                issue >= :begints and issue < :endts and expire < :endts
                and significance = 'W'
                and phenomena = ANY(:phenomena)
                {flimiter}
                GROUP by w.wfo, phenomena, eventid, significance, year,
                fcster),
            agg as (
                SELECT ST_SetSRID(ST_intersection(
                    ST_buffer(ST_exteriorring(
                        ST_geometryn(ST_multi(c.geom),1)),0.02),
                    ST_exteriorring(ST_geometryn(
                        ST_multi(s.geom),1))), 4326) as geo,
                c.year, c.wfo, c.phenomena, c.significance, c.eventid
                from stormbased s, countybased c WHERE
                s.wfo = c.wfo and s.eventid = c.eventid and
                s.phenomena = c.phenomena and s.significance = c.significance
                and s.year = c.year
            )

            SELECT sum(ST_Length(ST_transform(geo,2163))) as s,
            year || wfo || eventid || phenomena || significance ||
            '1' as key
            from agg GROUP by key
        """,
                wlimit=wlimit,
                taglimiter=self.sql_tag_limiter(),
                flimiter=self.sql_fcster_limiter(),
            ),
            dbconn,
            params={
                "begints": self.begints,
                "endts": self.endts,
                "phenomena": self.phenomena,
                "wfos": self.wfo,
                "fcster": self.fcster,
                "wind": self.wind,
                "hailsize": self.hailsize,
            },
            index_col="key",
        )
        self.events["sharedborder"] = df["s"]

    def sbw_verify(self):
        """Verify the events"""
        if self.stormreports_buffered is None or self.events_buffered is None:
            return
        centroids = self.stormreports_buffered.centroid
        for eidx, geometry in self.events_buffered.items():
            _ev = self.events.loc[eidx]
            # Prevent dups?
            if isinstance(_ev, pd.DataFrame):
                _ev = _ev.iloc[0]
            indicies = (self.stormreports["valid"] >= _ev["issue"]) & (
                self.stormreports["valid"] <= _ev["expire"]
            )
            # NB the within operation returns a boolean series sometimes false
            for sidx, isinside in centroids[indicies].within(geometry).items():
                if not isinside:
                    continue
                # No matter the below, this storm report is within bounds
                # so build the cross reference between the two
                self.events.at[eidx, "stormreports_all"].append(sidx)
                self.stormreports.at[sidx, "events"].append(eidx)

                _sr = self.stormreports.loc[sidx]
                verify = False
                if _ev["phenomena"] == "FF" and _sr["type"] in ["F", "x"]:
                    verify = True
                elif _ev["phenomena"] == "TO":
                    if _sr["type"] == "T":
                        verify = True
                    else:
                        # Only TDQ is not already warned
                        if not _sr["warned"]:
                            self.stormreports.at[sidx, "tdq"] = True
                elif _ev["phenomena"] == "DS":
                    if _sr["type"] == "2":
                        verify = True
                elif _ev["phenomena"] == "SQ" and _sr["type"] == "q":
                    verify = True
                elif _ev["phenomena"] == "MA" and _sr["type"] in [
                    "W",
                    "M",
                    "H",
                    "h",
                ]:
                    verify = True
                elif (
                    _ev["phenomena"] == "SV"
                    and _sr["type"] == "T"
                    and _ev["svr_tornado_possible"]
                ):
                    # Special accounting for tornados within tor possible SVRs
                    self.events.at[eidx, TOR_IN_SVRTORPOSSIBLE] = True
                elif _ev["phenomena"] == "SV" and _sr["type"] in [
                    "G",
                    "D",
                    "H",
                ]:
                    # If we are to verify based on the windhag tag, then we
                    # need to compare the magnitudes
                    if self.windhailtag:
                        if (
                            _sr["type"] == "H"
                            and _ev["hailtag"] is not None
                            and _sr["magnitude"] >= _ev["hailtag"]
                        ):
                            verify = True
                        elif (
                            _sr["type"] == "G"
                            and _ev["windtag"] is not None
                            and _sr["magnitude"] >= _ev["windtag"]
                        ):
                            verify = True
                        elif _sr["type"] == "D":  # can't tag verify these
                            verify = True
                    else:
                        verify = True
                if not verify:
                    continue
                self.events.at[eidx, "verify"] = True
                self.stormreports.at[sidx, "tdq"] = False
                self.stormreports.at[sidx, "warned"] = True
                leadtime = int(
                    (_sr["valid"] - _ev["issue"]).total_seconds() / 60.0
                )
                if _sr["leadtime"] is None:
                    self.stormreports.at[sidx, "leadtime"] = leadtime
                if not _ev["stormreports"]:
                    self.events.at[eidx, "lead0"] = leadtime
                self.events.at[eidx, "stormreports"].append(sidx)

    def area_verify(self):
        """Do Areal verification"""
        if self.events_buffered is None:
            return
        e2163 = self.events.to_crs(epsg=2163)  # type: ignore
        for eidx, _ev in e2163.iterrows():
            if not _ev["stormreports"]:
                continue
            # Union all the LSRs into one shape
            lsrs = unary_union(self.stormreports_buffered[_ev["stormreports"]])
            # Intersect with this warning geometry to find overlap
            overlap = _ev["geom"].buffer(0).intersection(lsrs)
            self.events.loc[eidx, "areaverify"] = overlap.area / 1000000.0

    def clean_dataframes(self):
        """Get rid of types we can not handle"""
        for df in [self.events, self.stormreports]:
            for colname in df.select_dtypes(
                include=["datetime64[ns]"]
            ).columns:
                df[colname] = df[colname].dt.strftime(ISO8601)

        def _to_csv(val):
            """helper."""
            return ",".join([str(s) for s in val])

        # Convert hacky column of lists to csv
        for s in ["", "_all"]:
            self.events[f"stormreports{s}"] = self.events[
                f"stormreports{s}"
            ].apply(_to_csv)
        self.stormreports["events"] = self.stormreports["events"].apply(
            _to_csv
        )


def handler(
    wfo,
    begints,
    endts,
    phenomena,
    lsrtype,
    hailsize,
    lsrbuffer,
    warningbuffer,
    wind,
    windhailtag,
    limitwarns,
    fcster,
    enable_shared_border,
):
    """Handle the request, return dict"""
    cow = COWSession(
        wfo,
        begints,
        endts,
        phenomena,
        lsrtype,
        hailsize,
        lsrbuffer,
        warningbuffer,
        wind,
        windhailtag,
        limitwarns,
        fcster,
        enable_shared_border,
    )
    cow.milk()
    # Some stuff is not JSON serializable
    cow.clean_dataframes()
    res = {
        "generated_at": utc().strftime(ISO8601),
        "params": {
            "wfo": cow.wfo,
            "phenomena": cow.phenomena,
            "lsrtype": cow.lsrtype,
            "hailsize": cow.hailsize,
            "lsrbuffer": cow.lsrbuffer,
            "wind": cow.wind,
            "windhailtag": cow.windhailtag,
            "limitwarns": cow.limitwarns,
            "begints": cow.begints.strftime(ISO8601),
            "endts": cow.endts.strftime(ISO8601),
            "warningbuffer": cow.warningbuffer,
            "enable_shared_border": cow.enable_shared_border,
        },
        "stats": cow.stats,
        "events": json.loads(cow.events.to_json()),
        "stormreports": json.loads(cow.stormreports.to_json()),
    }
    # only include when the easter egg is enabled
    if cow.fcster:
        res["params"]["fcster"] = cow.fcster

    return res


@router.get(
    "/cow.json",
    description=__doc__,
    tags=[
        "vtec",
    ],
)
def cow_service(
    wfo: List[str] = Query([], title="WFO Identifiers"),
    begints: datetime = Query(...),
    endts: datetime = Query(...),
    phenomena: List[str] = Query([]),
    lsrtype: List[str] = Query([]),
    hailsize: float = Query(1),
    lsrbuffer: float = Query(15),
    warningbuffer: float = Query(1),
    wind: float = Query(58),
    windhailtag: bool = Query(default=False),
    limitwarns: bool = Query(default=False),
    fcster: str = None,
    enable_shared_border: Optional[bool] = Query(
        default=None, title="Enable Shared Border calculation"
    ),
):
    """Replaced by __doc__."""
    if enable_shared_border is None:
        # When ambiguous, set it to False for requests > 1 yr * wfos
        enable_shared_border = (len(wfo) * (endts - begints).days) < 365
    return handler(
        wfo,
        begints,
        endts,
        phenomena,
        lsrtype,
        hailsize,
        lsrbuffer,
        warningbuffer,
        wind,
        windhailtag,
        limitwarns,
        fcster,
        enable_shared_border,
    )


cow_service.__doc__ = __doc__
