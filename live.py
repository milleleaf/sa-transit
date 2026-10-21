"""Adelaide Metro GTFS-realtime (CC BY 4.0): one GET of each feed per run (15 min). FEEDS: name -> endpoint under /v1/realtime/
  vehicles      vehicle_positions
  trip_updates  trip_updates
  alerts        service_alerts
Bucket, for each <name> (overwritten in place):
  live/<name>.json                the latest feed decoded to JSON, every field (including the feed's own TfNSW vehicle
                                  extension: air_conditioned, wheelchair_accessible, on every VehicleDescriptor as
                                  tfnsw_vehicle_descriptor)
  live/<name>.pb                  the latest feed as received (protobuf)
  live/<name>-YYYY-MM-DD.parquet  that UTC day's snapshots so far, appended each run: one row per entity per snapshot, the main
                                  fields as columns and `entity` = the full entity as JSON (vehicles: GeoParquet)
kaggle_archive.py moves each complete month to its own Kaggle dataset, and only then removes its days from the bucket.
On any error a feed is skipped for this run (a warning, exit 0): no retry loop. Needs HF_TOKEN. Usage: python3 live.py"""
import datetime, json, os, sys, tempfile, urllib.request
import duckdb
from google.protobuf import unknown_fields
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from huggingface_hub import batch_bucket_files, create_bucket, download_bucket_files

BASE = 'https://gtfs.adelaidemetro.com.au/v1/realtime/'
UA = {'User-Agent': 'AlreadyOpen sa-transit (https://github.com/milleleaf/sa-transit)'}
REPO = 'milleleaf/sa-transit'
EXT = 1999                                       # TfnswVehicleDescriptor on VehicleDescriptor (adelaidemetro_gtfsr.proto)
SNAP = "(SELECT (json->'header'->>'timestamp')::BIGINT AS snapshot, unnest(json_extract(json, '$.entity[*]')) AS e FROM read_json_objects('{src}'))"
VEHICLES = """
SELECT to_timestamp(snapshot) AS "snapshot", e->>'id' AS "entity_id",
  e->'vehicle'->'trip'->>'trip_id' AS "trip_id", e->'vehicle'->'trip'->>'route_id' AS "route_id",
  (e->'vehicle'->'trip'->>'direction_id')::INTEGER AS "direction_id", e->'vehicle'->'trip'->>'start_date' AS "start_date",
  e->'vehicle'->'trip'->>'start_time' AS "start_time", e->'vehicle'->'trip'->>'schedule_relationship' AS "schedule_relationship",
  e->'vehicle'->'vehicle'->>'id' AS "vehicle_id", e->'vehicle'->'vehicle'->>'label' AS "vehicle_label",
  e->'vehicle'->'vehicle'->>'license_plate' AS "license_plate",
  (e->'vehicle'->'vehicle'->'tfnsw_vehicle_descriptor'->>'air_conditioned')::BOOLEAN AS "air_conditioned",
  (e->'vehicle'->'vehicle'->'tfnsw_vehicle_descriptor'->>'wheelchair_accessible')::INTEGER AS "wheelchair_accessible",
  (e->'vehicle'->'position'->>'latitude')::DOUBLE AS "lat", (e->'vehicle'->'position'->>'longitude')::DOUBLE AS "lon",
  (e->'vehicle'->'position'->>'bearing')::DOUBLE AS "bearing", (e->'vehicle'->'position'->>'speed')::DOUBLE AS "speed",
  (e->'vehicle'->'position'->>'odometer')::DOUBLE AS "odometer",
  (e->'vehicle'->>'current_stop_sequence')::INTEGER AS "current_stop_sequence", e->'vehicle'->>'stop_id' AS "stop_id",
  e->'vehicle'->>'current_status' AS "current_status", to_timestamp((e->'vehicle'->>'timestamp')::BIGINT) AS "timestamp",
  e->'vehicle'->>'congestion_level' AS "congestion_level", e->'vehicle'->>'occupancy_status' AS "occupancy_status",
  (e->'vehicle'->>'occupancy_percentage')::INTEGER AS "occupancy_percentage", e AS "entity",
  ST_Point((e->'vehicle'->'position'->>'longitude')::DOUBLE, (e->'vehicle'->'position'->>'latitude')::DOUBLE) AS "geometry"
FROM """ + SNAP
TRIP_UPDATES = """
SELECT to_timestamp(snapshot) AS "snapshot", e->>'id' AS "entity_id",
  e->'trip_update'->'trip'->>'trip_id' AS "trip_id", e->'trip_update'->'trip'->>'route_id' AS "route_id",
  (e->'trip_update'->'trip'->>'direction_id')::INTEGER AS "direction_id", e->'trip_update'->'trip'->>'start_date' AS "start_date",
  e->'trip_update'->'trip'->>'start_time' AS "start_time",
  e->'trip_update'->'trip'->>'schedule_relationship' AS "schedule_relationship",
  e->'trip_update'->'vehicle'->>'id' AS "vehicle_id", e->'trip_update'->'vehicle'->>'label' AS "vehicle_label",
  to_timestamp((e->'trip_update'->>'timestamp')::BIGINT) AS "timestamp", (e->'trip_update'->>'delay')::INTEGER AS "delay",
  json_array_length(e->'trip_update'->'stop_time_update') AS "stop_time_updates", e AS "entity"
FROM """ + SNAP
ALERTS = """
SELECT to_timestamp(snapshot) AS "snapshot", e->>'id' AS "entity_id", e->'alert'->>'cause' AS "cause",
  e->'alert'->>'effect' AS "effect", e->'alert'->>'severity_level' AS "severity_level",
  e->'alert'->'header_text'->'translation'->0->>'text' AS "header_text",
  e->'alert'->'description_text'->'translation'->0->>'text' AS "description_text",
  e->'alert'->'url'->'translation'->0->>'text' AS "url", e->'alert'->'active_period' AS "active_period",
  e->'alert'->'informed_entity' AS "informed_entity", e AS "entity"
FROM """ + SNAP
FEEDS = {'vehicles': ('vehicle_positions', VEHICLES), 'trip_updates': ('trip_updates', TRIP_UPDATES), 'alerts': ('service_alerts', ALERTS)}


def varints(b):
    """a length-delimited sub-message of varint fields -> {field number: value}"""
    out, i = {}, 0
    while i < len(b):
        vals = []
        for _ in range(2):                                             # the tag, then the value
            v = s = 0
            while True:
                x = b[i]; i += 1; v |= (x & 0x7f) << s; s += 7
                if x < 0x80: break
            vals.append(v)
        assert vals[0] & 7 == 0, 'extension holds a non-varint field'
        out[vals[0] >> 3] = vals[1]
    return out


def decode(body):
    """protobuf -> dict with every field, the TfNSW extension merged into every VehicleDescriptor; any other unknown field
    becomes a warning (it stays in the .pb only)"""
    f = gtfs_realtime_pb2.FeedMessage(); f.ParseFromString(body)
    d = MessageToDict(f, preserving_proto_field_name=True)
    def walk(m, md, path):
        for u in unknown_fields.UnknownFieldSet(m):
            if m.DESCRIPTOR.name == 'VehicleDescriptor' and u.field_number == EXT:
                x = varints(u.data)
                md['tfnsw_vehicle_descriptor'] = {**({'air_conditioned': bool(x[1])} if 1 in x else {}),
                                                  **({'wheelchair_accessible': x[2]} if 2 in x else {})}
            else:
                print(f'::warning::unknown field {u.field_number} at {path}: not decoded (only in the .pb)')
        for fd, v in m.ListFields():
            if fd.message_type:
                sub = md.setdefault(fd.name, [] if fd.is_repeated else {})
                for i, x in enumerate(v if fd.is_repeated else [v]): walk(x, sub[i] if fd.is_repeated else sub, f'{path}.{fd.name}')
    walk(f, d, 'feed')
    return d


def snapshot(name, w, con):
    """fetch and decode one feed; returns the bucket files to add (local, remote) and a summary, or ([], None) on any error"""
    endpoint, rows_sql = FEEDS[name]
    try:
        with urllib.request.urlopen(urllib.request.Request(BASE + endpoint, headers=UA), timeout=60) as r:
            body = r.read()
        d = decode(body)
        assert d.get('header', {}).get('timestamp'), f'no header timestamp: {str(d)[:200]}'
    except Exception as e:                                             # 5xx, timeout, bad protobuf: wait for the next run
        print(f'::warning::Adelaide Metro {endpoint}: {e}; skipping it this run'); return [], None
    day = datetime.datetime.fromtimestamp(int(d['header']['timestamp']), datetime.UTC).date()
    remote = f'live/{name}-{day}.parquet'
    snap, pb, old, out = (os.path.join(w, f'{name}{x}') for x in ('.json', '.pb', '-old.parquet', '-new.parquet'))
    json.dump(d, open(snap, 'w'), separators=(',', ':')); open(pb, 'wb').write(body)
    download_bucket_files(REPO, [(remote, old)], raise_on_missing_files=False)   # absent on the day's first run
    new = rows_sql.format(src=snap)
    q = f"SELECT * FROM read_parquet('{old}') UNION ALL BY NAME {new}" if os.path.exists(old) else new
    if d.get('entity'):
        con.execute(f"COPY ({q} ORDER BY snapshot, entity_id) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
    rows = con.execute(f"SELECT count(*), count(DISTINCT snapshot) FROM '{out}'").fetchone() if os.path.exists(out) else (0, 0)
    add = [(snap, f'live/{name}.json'), (pb, f'live/{name}.pb')] + ([(out, remote)] if os.path.exists(out) else [])
    return add, {'feed': name, 'snapshot': d['header']['timestamp'], 'entities': len(d.get('entity', [])), 'file': remote,
                 'rows': rows[0], 'snapshots': rows[1]}


def main():
    w = tempfile.mkdtemp()
    create_bucket(REPO, private=False, exist_ok=True)                    # the first snapshot may run before the first bake
    con = duckdb.connect(); con.execute('INSTALL spatial; LOAD spatial')
    add, done = [], []
    for name in FEEDS:
        a, s = snapshot(name, w, con); add += a
        if s: done.append(s)
    if add: batch_bucket_files(REPO, add=add)
    print(json.dumps(done))


if __name__ == '__main__':
    if sys.argv[1:] == ['test']:                                       # the extension decoder, no network
        assert varints(bytes([0x08, 0x01, 0x10, 0x01])) == {1: 1, 2: 1} and varints(bytes([0x10, 0xac, 0x02])) == {2: 300}
        vd = gtfs_realtime_pb2.VehicleDescriptor(id='v').SerializeToString() + bytes([0xfa, 0x7c, 4, 0x08, 0x01, 0x10, 0x01])
        f = gtfs_realtime_pb2.FeedMessage(); f.header.gtfs_realtime_version = '2.0'
        a, b = f.entity.add(id='a'), f.entity.add(id='b')
        a.vehicle.vehicle.MergeFromString(vd); b.trip_update.trip.trip_id = 't'; b.trip_update.vehicle.MergeFromString(vd)
        e = decode(f.SerializeToString())['entity']
        want = {'air_conditioned': True, 'wheelchair_accessible': 1}
        assert e[0]['vehicle']['vehicle']['tfnsw_vehicle_descriptor'] == want and e[1]['trip_update']['vehicle']['tfnsw_vehicle_descriptor'] == want, e
        print('live: self-test ok')
    elif len(sys.argv) > 1:                                            # decode a saved feed, no network: python3 live.py feed.pb
        print(json.dumps(decode(open(sys.argv[1], 'rb').read()), indent=1)[:3000])
    else:
        main()
