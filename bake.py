"""South Australia public transport from the Adelaide Metro GTFS (CC BY 4.0; Adelaide Metro buses, trains and trams plus the
regional coach operators in the same feed, e.g. Stateliner), as GeoParquet, JSON and vector tiles. Every row and field is kept.
  gtfs/<table>.parquet    every table of the feed (agency, routes, trips, stop_times, shapes, ...), every column as text,
                          exactly as published (no type guessing: ids keep leading zeros, times past 24:00 stay as written)
  release-notes.txt       the feed's own release notes, copied
  stops.parquet           GeoParquet: stops.txt, every column, + geometry (a point; null for a stop without coordinates)
  route-shapes.parquet    GeoParquet: one row per (route, shape) that a trip uses: every routes.txt column, shape_id, trips
                          (trips on that route with that shape), points, shape_dist_traveled (the shape's last value),
                          geometry (the shape as a line, in shape_pt_sequence order)
  stops.json              GeoJSON of every stop with coordinates, every field (for the map)
  routes.json             every route, every field, with its shape_ids and trip count (for the map)
  transit.pmtiles         layers `routes` (route-shapes) and `stops`, zooms 4 to 14, every feature in every zoom
index.json counts the rows of each table and the features of each tile layer at zoom 14.
Usage: python3 bake.py"""
import datetime, json, os, re, shutil, subprocess, urllib.request, zipfile
import duckdb
import sources

URL = 'https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip'
UA = {'User-Agent': 'AlreadyOpen sa-transit (https://github.com/milleleaf/sa-transit)'}
NOT_CSV = {'Release Notes.txt'}                                         # every other .txt in the zip is a GTFS table
MINZOOM, MAXZOOM = 4, 14


def download():
    p = os.path.join(sources.CACHE, 'google_transit.zip')
    with urllib.request.urlopen(urllib.request.Request(URL, headers=UA), timeout=600) as r:
        open(p + '.part', 'wb').write(r.read())
    os.replace(p + '.part', p)
    return p


def deepest(pmtiles, layer, zoom=MAXZOOM):
    """distinct `fid` of one layer at one zoom (a feature crossing tiles appears in each); streamed, never held in memory"""
    ids, pat = set(), re.compile(r'"fid": ?(\d+)')
    with subprocess.Popen(['tippecanoe-decode', '-l', layer, '-z', str(zoom), '-Z', str(zoom), pmtiles],
                          stdout=subprocess.PIPE, text=True) as p:
        for line in p.stdout:
            if '"Feature"' in line and (m := pat.search(line)): ids.add(int(m.group(1)))
    assert p.returncode == 0, f'tippecanoe-decode failed on {pmtiles}'
    return len(ids)


def geojsonl(con, sql, out):
    """rows of sql (a `geom` GeoJSON text column and an integer `fid`) as GeoJSON lines; nulls left out"""
    cur = con.execute(sql); cols = [d[0] for d in cur.description]; n = 0
    with open(out, 'w') as f:
        while rows := cur.fetchmany(10000):
            for r in rows:
                p = dict(zip(cols, r)); g = json.loads(p.pop('geom'))
                f.write(json.dumps({'type': 'Feature', 'geometry': g, 'properties': {k: v for k, v in p.items() if v is not None}}) + '\n'); n += 1
    return n


def main():
    for d in (sources.CACHE, sources.OUT): os.makedirs(d, exist_ok=True)
    z = zipfile.ZipFile(download())
    src = os.path.join(sources.DATA, 'gtfs'); shutil.rmtree(src, ignore_errors=True); z.extractall(src)
    o = lambda f: os.path.join(sources.OUT, f); os.makedirs(o('gtfs'), exist_ok=True)
    con = duckdb.connect(); con.execute('INSTALL spatial; LOAD spatial')
    counts, tables = {}, []
    for name in sorted(z.namelist()):
        if name in NOT_CSV:
            shutil.copyfile(os.path.join(src, name), o(name.lower().replace(' ', '-'))); continue
        t = os.path.splitext(name)[0]; tables.append(t)
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_csv('{os.path.join(src, name)}', all_varchar=true, header=true, nullstr='')")
        con.execute(f"COPY {t} TO '{o(f'gtfs/{t}.parquet')}' (FORMAT parquet, COMPRESSION zstd)")
        counts[t] = con.execute(f'SELECT count(*) FROM {t}').fetchone()[0]
    for t in ['stops', 'routes', 'trips', 'shapes']:
        assert t in tables, f'{t}.txt missing from the feed'
    con.execute("""CREATE TABLE stops_g AS SELECT *, CASE WHEN stop_lat IS NOT NULL AND stop_lon IS NOT NULL
                   THEN ST_Point(stop_lon::DOUBLE, stop_lat::DOUBLE) END AS geometry FROM stops""")
    con.execute("""CREATE TABLE lines AS SELECT shape_id, count(*) AS points, max(shape_dist_traveled::DOUBLE) AS shape_dist_traveled,
                   ST_MakeLine(list(ST_Point(shape_pt_lon::DOUBLE, shape_pt_lat::DOUBLE) ORDER BY shape_pt_sequence::INTEGER)) AS geometry
                   FROM shapes GROUP BY shape_id""")
    con.execute("""CREATE TABLE route_shapes AS SELECT r.*, l.shape_id, u.trips, l.points, l.shape_dist_traveled, l.geometry
                   FROM (SELECT route_id, shape_id, count(*) AS trips FROM trips WHERE shape_id IS NOT NULL GROUP BY ALL) u
                   JOIN routes r USING (route_id) JOIN lines l USING (shape_id) ORDER BY route_id, shape_id""")
    con.execute(f"COPY stops_g TO '{o('stops.parquet')}' (FORMAT parquet, COMPRESSION zstd)")
    con.execute(f"COPY route_shapes TO '{o('route-shapes.parquet')}' (FORMAT parquet, COMPRESSION zstd)")
    counts['route_shapes'] = con.execute('SELECT count(*) FROM route_shapes').fetchone()[0]
    counts['shape_lines'] = con.execute('SELECT count(*) FROM lines').fetchone()[0]
    counts['stops_located'] = con.execute('SELECT count(*) FROM stops_g WHERE geometry IS NOT NULL').fetchone()[0]
    unused = con.execute('SELECT count(*) FROM lines WHERE shape_id NOT IN (SELECT shape_id FROM route_shapes)').fetchone()[0]
    assert unused == 0, f'{unused} shapes are used by no trip: they would be missing from route-shapes'

    # JSON for the map: stops as GeoJSON, routes with their shapes and trip counts
    rows = lambda sql: (lambda c: [dict(zip([d[0] for d in c.description], r)) for r in c.fetchall()])(con.execute(sql))
    stops = [{'type': 'Feature', 'geometry': json.loads(s.pop('geom')), 'properties': {k: v for k, v in s.items() if v is not None}}
             for s in rows('SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geom FROM stops_g WHERE geometry IS NOT NULL')]
    json.dump({'type': 'FeatureCollection', 'features': stops}, open(o('stops.json'), 'w'), separators=(',', ':'))
    routes = rows("""SELECT r.*, coalesce(s.shape_ids, []) AS shape_ids, coalesce(t.trips, 0) AS trips FROM routes r
                     LEFT JOIN (SELECT route_id, list(shape_id ORDER BY shape_id) AS shape_ids FROM route_shapes GROUP BY ALL) s USING (route_id)
                     LEFT JOIN (SELECT route_id, count(*) AS trips FROM trips GROUP BY ALL) t USING (route_id) ORDER BY route_id""")
    json.dump(routes, open(o('routes.json'), 'w'), separators=(',', ':'))

    tmp = os.path.join(sources.DATA, 'tmp'); os.makedirs(tmp, exist_ok=True)
    rl, sl = os.path.join(tmp, 'routes.geojsonl'), os.path.join(tmp, 'stops.geojsonl')
    nr = geojsonl(con, 'SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geom, row_number() OVER () - 1 AS fid FROM route_shapes', rl)
    ns = geojsonl(con, 'SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geom, row_number() OVER () - 1 AS fid FROM stops_g WHERE geometry IS NOT NULL', sl)
    subprocess.run(['tippecanoe', '-o', o('transit.pmtiles'), '--force', f'-Z{MINZOOM}', f'-z{MAXZOOM}', '-r1', '--no-feature-limit',
                    '--no-tile-size-limit', '--quiet', '-L', f'routes:{rl}', '-L', f'stops:{sl}'], check=True)
    counts['routes_tiles_deepest_zoom'] = deepest(o('transit.pmtiles'), 'routes')
    counts['stops_tiles_deepest_zoom'] = deepest(o('transit.pmtiles'), 'stops')
    shutil.rmtree(tmp); shutil.rmtree(src)
    assert counts['routes_tiles_deepest_zoom'] == nr == counts['route_shapes'], f'routes layer: {counts["routes_tiles_deepest_zoom"]} of {nr} at z{MAXZOOM}'
    assert counts['stops_tiles_deepest_zoom'] == ns == counts['stops_located'], f'stops layer: {counts["stops_tiles_deepest_zoom"]} of {ns} at z{MAXZOOM}'
    feed = con.execute('SELECT * FROM feed_info').fetchone() if 'feed_info' in tables else None
    idx = {'built': datetime.datetime.now().isoformat(timespec='seconds'), 'counts': counts, 'tables': tables,
           'feed_info': dict(zip([d[0] for d in con.description], feed)) if feed else None}
    json.dump(idx, open(o('index.json'), 'w'), indent=1)
    print(json.dumps(idx))


if __name__ == '__main__':
    main()
