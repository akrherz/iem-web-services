"""IEM Cow API

/api/1/cow.json?
  wfo=ABC&wfo=DEF

"""
import datetime
import json
import sys

import pytz
import geopandas as gpd
from shapely.ops import cascaded_union
from pyiem.util import utc, get_dbconn
ISO9660 = "%Y-%m-%dT%H:%M:%SZ"


def printt(msg):
    """Print a message with a timestamp included"""
    if sys.stdout.isatty():
        print(("%s %s"
               ) % (datetime.datetime.now().strftime("%H:%M:%S.%f"), msg))


def compute_times(fields):
    """Figure out our start and end times"""
    if 'begints' in fields:
        begints = datetime.datetime.strptime(fields['begints'][:16],
                                             ISO9660[:14])
        begints = begints.replace(tzinfo=pytz.utc)
        endts = datetime.datetime.strptime(fields['endts'][:16], ISO9660[:14])
        endts = endts.replace(tzinfo=pytz.utc)
    else:
        ets = datetime.datetime.utcnow()
        sts = ets - datetime.timedelta(hours=4)
        begints = utc(int(fields.get('syear', sts.year)),
                      int(fields.get('smonth', sts.month)),
                      int(fields.get('sday', sts.day)),
                      int(fields.get('shour', sts.hour)))
        endts = utc(int(fields.get('eyear', ets.year)),
                    int(fields.get('emonth', ets.month)),
                    int(fields.get('eday', ets.day)),
                    int(fields.get('ehour', ets.hour)))
    return begints, endts


class COWSession(object):
    """Things that we could do while generating Cow stats"""

    def __init__(self, fields):
        """Build out our session based on provided fields"""
        self.wfo = fields.getall('wfo')
        # Figure out the begin and end times
        self.begints, self.endts = compute_times(fields)
        # Storage of data
        self.events = gpd.GeoDataFrame()
        self.events_buffered = None
        self.stormreports = gpd.GeoDataFrame()
        self.stormreports_buffered = None
        self.stats = dict()
        # query parameters
        self.phenomena = fields.getall('phenomena')
        if not self.phenomena:
            self.phenomena = ['TO', 'SV', 'FF', 'MA']
        self.lsrtype = fields.getall('lsrtype')
        if not self.lsrtype:
            self.lsrtype = ['TO', 'SV', 'FF', 'MA']
        self.hailsize = float(fields.get('hailsize', 1.0))
        self.lsrbuffer = float(fields.get('lsrbuffer', 15))
        self.warningbuffer = float(fields.get('warningbuffer', 1))
        self.wind = float(fields.get('wind', 58))
        self.windhailtag = fields.get('windhailtag', 'N').upper() == 'Y'
        self.limitwarns = fields.get('limitwarns', 'N').upper() == 'Y'
        self.fcster = fields.get('fcster', None)
        # our database connection
        self.dbconn = get_dbconn('postgis')

    def milk(self):
        """Milk the Cow and see what happens"""
        self.load_events()
        self.load_stormreports()
        self.compute_shared_border()
        self.sbw_verify()
        self.area_verify()
        self.compute_stats()

    def compute_stats(self):
        """Fill out the stats attribute"""
        printt("compute_stats called...")
        _ev = self.events
        _sr = self.stormreports
        self.stats['area_verify[%]'] = (
            0 if _ev.empty else
            _ev['areaverify'].sum() / _ev['parea'].sum() * 100.
        )
        self.stats['shared_border[%]'] = (
            0 if _ev.empty else
            _ev['sharedborder'].sum() / _ev['perimeter'].sum() * 100.
        )
        self.stats['max_leadtime[min]'] = (
            None if _sr.empty else
            _sr['leadtime'].max()
        )
        self.stats['min_leadtime[min]'] = (
            None if _sr.empty else
            _sr['leadtime'].min()
        )
        self.stats['avg_leadtime[min]'] = (
            None if _sr.empty else
            _sr['leadtime'].mean()
        )
        self.stats['tdq_stormreports'] = (
            0 if _sr.empty else
            len(_sr[_sr['tdq']].index))
        self.stats['unwarned_reports'] = (
            0 if _sr.empty else
            len(_sr[~ _sr['warned']].index)
        )
        self.stats['warned_reports'] = (
            0 if _sr.empty else
            len(_sr[_sr['warned']].index)
        )
        self.stats['events_verified'] = (
            0 if _ev.empty else
            len(_ev[_ev['verify']].index)
        )
        self.stats['events_total'] = len(_ev.index)
        self.stats['reports_total'] = len(_sr.index)
        if self.stats['reports_total'] > 0:
            pod = (self.stats['warned_reports'] /
                   float(self.stats['reports_total']))
        else:
            pod = 0
        self.stats['POD[1]'] = pod
        if self.stats['events_total'] > 0:
            far = (self.stats['events_total'] -
                   self.stats['events_verified']) / self.stats['events_total']
        else:
            far = 0
        self.stats['FAR[1]'] = far
        if far > 0 and pod > 0:
            self.stats['CSI[1]'] = (((pod)**-1 + (1 - far)**-1) - 1)**-1
        else:
            self.stats['CSI[1]'] = 0.
        self.stats['avg_size[sq km]'] = (
            0 if _ev.empty else _ev['parea'].mean()
        )
        self.stats['size_poly_vs_county[%]'] = (
            0 if _ev.empty else
            _ev['parea'].sum() / _ev['carea'].sum() * 100.
        )

    def sql_lsr_limiter(self):
        """How to limit LSR types"""
        ltypes = []
        if 'TO' in self.lsrtype:
            ltypes.append('T')
        if 'SV' in self.lsrtype:
            ltypes.extend(['H', 'G', 'D'])
        if 'FF' in self.lsrtype:
            ltypes.extend(['F', 'x'])
        if 'MA' in self.lsrtype:
            ltypes.extend(['M', 'W'])
        if len(ltypes) == 1:
            return " and type = '%s'" % (ltypes[0], )
        return " and type in %s " % (tuple(ltypes), )

    def sql_fcster_limiter(self):
        """Should we limit the fcster column?"""
        if self.fcster is None:
            return " "
        return " and fcster = '%s' " % (self.fcster, )

    def sql_wfo_limiter(self):
        """get the SQL for how we limit WFOs"""
        if "_ALL" in self.wfo or not self.wfo:
            return " "
        if len(self.wfo) == 1:
            return " and w.wfo = '%s' " % (self.wfo[0], )
        return " and w.wfo in %s " % (tuple(self.wfo), )

    def sql_tag_limiter(self):
        """Do we need to limit the events based on tags"""
        if not self.limitwarns:
            return " "
        return (" and ((s.windtag >= %s or s.hailtag >= %s) or "
                " (s.windtag is null and s.hailtag is null)) "
                ) % (self.wind, self.hailsize)

    def load_events(self):
        """Build out the listing of events based on the request"""
        printt("load_events called...")
        self.events = gpd.read_postgis("""
        WITH stormbased as (
            SELECT wfo, phenomena, eventid, hailtag, windtag,
            geom, significance,
            ST_area(ST_transform(geom,2163)) / 1000000.0 as parea,
            ST_perimeter(ST_transform(geom,2163)) as perimeter,
            ST_xmax(geom) as lon0, ST_ymax(geom) as lat0,
            extract(year from issue at time zone 'UTC') as year
            from sbw w WHERE status = 'NEW' """ + self.sql_wfo_limiter() + """
            and issue >= %s and issue < %s and expire < %s
            and phenomena in %s """ + self.sql_tag_limiter() + """
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
            w.gid is not null """ + self.sql_wfo_limiter() + """ and
            issue >= %s and issue < %s and expire < %s
            and phenomena in %s """ + self.sql_tag_limiter() + """
            """ + self.sql_fcster_limiter() + """
            GROUP by w.wfo, phenomena, eventid, significance, year, fcster
        )
        SELECT s.year::int, s.wfo, s.phenomena, s.eventid, s.geom,
        c.missue as issue,
        c.mexpire as expire, c.statuses, c.fcster,
        s.significance, s.hailtag, s.windtag, c.carea, c.ar_ugc,
        s.lat0, s.lon0, s.perimeter, s.parea, c.ar_ugcname
        from stormbased s JOIN countybased c on
        (c.eventid = s.eventid and c.wfo = s.wfo and c.year = s.year
        and c.phenomena = s.phenomena and c.significance = s.significance)
        ORDER by issue ASC
        """, self.dbconn, params=(self.begints, self.endts, self.endts,
                                  tuple(self.phenomena),
                                  self.begints, self.endts,
                                  self.endts, tuple(self.phenomena)),
                                       crs={'init': 'epsg:4326'})
        if self.events.empty:
            return
        s2163 = self.events['geom'].to_crs(epsg=2163)
        self.events_buffered = s2163.buffer(self.warningbuffer * 1000.)
        self.events['stormreports'] = [[] for _ in range(
            len(self.events.index))]
        self.events['verify'] = False
        self.events['lead0'] = None
        self.events['areaverify'] = 0
        self.events['sharedborder'] = 0

    def load_stormreports(self):
        """Build out the listing of storm reports based on the request"""
        printt("load_stormreports called...")
        self.stormreports = gpd.read_postgis("""
        SELECT distinct valid at time zone 'UTC' as valid,
        type, magnitude, city, county, state,
        source, remark, wfo, typetext, ST_x(geom) as lon0, ST_y(geom) as lat0,
        geom
        from lsrs w WHERE valid >= %s and valid < %s
        """ + self.sql_wfo_limiter() + """ """ + self.sql_lsr_limiter() + """
        and ((type = 'M' and magnitude >= 34) or
        (type = 'H' and magnitude >= %s) or type = 'W' or
         type = 'T' or (type = 'G' and magnitude >= %s) or type = 'D'
         or type = 'F' or type = 'x') ORDER by valid ASC
        """, self.dbconn, params=(self.begints, self.endts,
                                  self.hailsize, self.wind), geom_col='geom',
                                             crs={'init': 'epsg:4326'})
        if self.stormreports.empty:
            return
        s2163 = self.stormreports['geom'].to_crs(epsg=2163)
        self.stormreports_buffered = s2163.buffer(self.lsrbuffer * 1000.)
        self.stormreports['events'] = [[] for _ in range(
            len(self.stormreports.index))]
        self.stormreports['tdq'] = False
        self.stormreports['warned'] = False
        self.stormreports['leadtime'] = None

    def compute_shared_border(self):
        """Compute a stat"""
        printt("compute_shared_border called...")
        cursor = self.dbconn.cursor()
        for eidx, row in self.events.iterrows():
            cursor.execute("""
    WITH stormbased as (
        SELECT geom from sbw_""" + str(row["year"]) + """
        where wfo = %s
        and eventid = %s and significance = %s
        and phenomena = %s and status = 'NEW'),
    countybased as (
        SELECT ST_Union(u.geom) as geom from
        warnings_""" + str(row["year"]) + """ w JOIN ugcs u on (u.gid = w.gid)
        WHERE w.wfo = %s and eventid = %s and
        significance = %s and phenomena = %s)

        SELECT sum(ST_Length(ST_transform(geo,2163))) as s from
            (SELECT ST_SetSRID(ST_intersection(
             ST_buffer(ST_exteriorring(ST_geometryn(ST_multi(c.geom),1)),0.02),
             ST_exteriorring(ST_geometryn(ST_multi(s.geom),1))), 4326) as geo
             from stormbased s, countybased c) as foo
            """, (row['wfo'], row['eventid'], row['significance'],
                  row['phenomena'], row['wfo'], row['eventid'],
                  row['significance'], row['phenomena']))
            if cursor.rowcount > 0:
                self.events.at[eidx, 'sharedborder'] = cursor.fetchone()[0]

    def sbw_verify(self):
        """Verify the events"""
        printt("sbw_verify called...")
        if self.stormreports_buffered is None:
            return
        centroids = self.stormreports_buffered.centroid
        for eidx, geometry in self.events_buffered.iteritems():
            _ev = self.events.loc[eidx]
            indicies = ((self.stormreports['valid'] >= _ev['issue']) &
                        (self.stormreports['valid'] < _ev['expire']))
            for sidx, _ in centroids[indicies].within(geometry).iteritems():
                _sr = self.stormreports.loc[sidx]
                if _sr['events']:
                    continue
                verify = False
                if _ev['phenomena'] == 'FF' and _sr['type'] in ['F', 'x']:
                    verify = True
                elif _ev['phenomena'] == 'TO':
                    if _sr['type'] == 'T':
                        verify = True
                    else:
                        self.stormreports.at[sidx, 'tdq'] = True
                elif (_ev['phenomena'] == 'MA' and
                        _sr['type'] in ['W', 'M', 'H']):
                    verify = True
                elif (_ev['phenomena'] == 'SV' and
                        _sr['type'] in ['G', 'D', 'H']):
                    verify = True
                if not verify:
                    continue
                self.events.at[eidx, 'verify'] = True
                self.stormreports.at[sidx, 'warned'] = True
                leadtime = int(
                    (_sr['valid'] - _ev['issue']).total_seconds() / 60.)
                if _sr['leadtime'] is None:
                    self.stormreports.at[sidx, 'leadtime'] = leadtime
                if not _ev['stormreports']:
                    self.events.at[eidx, 'lead0'] = leadtime
                self.events.at[eidx, 'stormreports'].append(sidx)
                self.stormreports.at[sidx, 'events'].append(eidx)

    def area_verify(self):
        """Do Areal verification"""
        printt("area_verify called...")
        if self.events_buffered is None:
            return
        for eidx, geometry in self.events_buffered.iteritems():
            _ev = self.events.loc[eidx]
            if not _ev['stormreports']:
                continue
            lsrs = cascaded_union(
                self.stormreports_buffered[_ev['stormreports']])
            overlap = geometry.intersection(lsrs)
            self.events.loc[eidx, 'areaverify'] = overlap.area / 1000000.

    def clean_dataframes(self):
        """Get rid of types we can not handle"""
        for df in [self.events, self.stormreports]:
            for colname in df.select_dtypes(
                    include=['datetime64[ns]']).columns:
                df[colname] = df[colname].dt.strftime(ISO9660)


def handler(_version, fields, _environ):
    """Handle the request, return dict"""
    cow = COWSession(fields)
    cow.milk()
    res = {'generated_at': datetime.datetime.utcnow().strftime(ISO9660),
           'params': {'wfo': cow.wfo, 'phenomena': cow.phenomena,
                      'lsrtype': cow.lsrtype, 'hailsize': cow.hailsize,
                      'lsrbuffer': cow.lsrbuffer, 'wind': cow.wind,
                      'windhailtag': cow.windhailtag,
                      'limitwarns': cow.limitwarns,
                      'warningbuffer': cow.warningbuffer},
           'stats': cow.stats,
           'events': "REPLACEME1",
           'stormreports': "REPLACEME2"}
    # only include when the easter egg is enabled
    if cow.fcster:
        res['params']['fcster'] = cow.fcster
    # Some stuff is not JSON serializable
    cow.clean_dataframes()
    # HACK as we need to do raw string subs
    return json.dumps(res).replace('"REPLACEME1"', cow.events.to_json()
                                   ).replace('"REPLACEME2"',
                                             cow.stormreports.to_json())


def test_empty():
    """Can we run when no data is found?"""
    from paste.util.multidict import MultiDict
    flds = MultiDict()
    flds.add('wfo', 'XXX')
    flds.add('begints', '2018-06-20T12:00')
    flds.add('endts', '2018-06-21T12:00')
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats['events_total'] == 0


def test_180620():
    """Compare with what we have from legacy PHP based Cow"""
    from paste.util.multidict import MultiDict
    flds = MultiDict()
    flds.add('wfo', 'DMX')
    flds.add('begints', '2018-06-20T12:00')
    flds.add('endts', '2018-06-21T12:00')
    flds.add('hailsize', 1.0)
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats['events_total'] == 18
    assert cow.stats['events_verified'] == 4
    assert abs(cow.stats['size_poly_vs_county[%]'] - 13.3) < 0.1
    # variance: PHP has this at 17.0
    assert abs(cow.stats['area_verify[%]'] - 11.35) < 0.1
    _ev = cow.events.iloc[0]
    assert abs(_ev['parea'] - 919.) < 1
    assert abs(_ev['parea'] / _ev['carea'] - 0.19) < 0.01


def test_one():
    """Compare with what we have from legacy PHP based Cow"""
    from paste.util.multidict import MultiDict
    flds = MultiDict()
    flds.add('wfo', 'DMX')
    flds.add('begints', '2018-06-18T12:00')
    flds.add('endts', '2018-06-20T12:00')
    flds.add('hailsize', 1.0)
    cow = COWSession(flds)
    cow.milk()
    assert cow.stats['events_total'] == 5
    assert cow.stats['events_verified'] == 2
    assert abs(cow.stats['size_poly_vs_county[%]'] - 24.3) < 0.1
    # variance: PHP has this at 15.0
    assert abs(cow.stats['area_verify[%]'] - 16.5) < 0.1
    _ev = cow.events.iloc[0]
    assert abs(_ev['parea'] - 950.) < 1
    assert abs(_ev['parea'] / _ev['carea'] - 0.159) < 0.01


def main():
    """A main func for testing"""
    from paste.util.multidict import MultiDict
    flds = MultiDict()
    flds.add('begints', '2018-01-01T12:00')
    flds.add('endts', '2018-06-21T12:00')
    flds.add('hailsize', 1.0)
    flds.add('wfo', 'DMX')
    js = json.loads(handler('1', flds, dict()))
    print(json.dumps(js['stats'], indent=2))
    for event in js['events']['features']:
        props = event['properties']
        print(("%s %3s %s %3s %s %7.2f %s"
               ) % (props['wfo'], props['eventid'],
                    props['issue'], props['expire'],
                    props['phenomena'], props['parea'], props['stormreports']))
    # print(json.dumps(js, indent=2))


if __name__ == '__main__':
    main()
