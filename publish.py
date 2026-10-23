"""Publish sources.OUT to Hugging Face (needs HF_TOKEN with write access to the alreadyopen org):
  dataset  REPO  DATASET_FILES + manifest.json + the card (hf/README.md): one commit a run, versioned (data tools)
  bucket   REPO  BUCKET_FILES + manifest.json, overwritten in place (read by browsers: CORS + HTTP Range); live.py adds live/*
Nothing is uploaded unless the publish gate passes (manifest.py). Adelaide Metro trims expired services from the feed, so the
timetable tables (TIMETABLE) are compared on rows whose service still runs on or after this bake's date, on both sides: the
published side is recounted from the published dataset's gtfs/*.parquet for that date (else its manifest's *_valid counts, else
the raw counts for that run, said in the log). Every other count keeps the raw comparison.
Usage: python3 publish.py     (after bake.py)   |   python3 publish.py test    (self-test, no data, no network)"""
import json, os, sys
import duckdb
from huggingface_hub import CommitOperationAdd, HfApi, batch_bucket_files, create_bucket
import bake, manifest, sources

REPO = 'milleleaf/sa-transit'
_idx = os.path.join(sources.OUT, 'index.json')
TABLES = json.load(open(_idx))['tables'] if os.path.exists(_idx) else []
BUCKET_FILES = ['index.json', 'transit.pmtiles', 'stops.json', 'routes.json', 'stops.parquet', 'route-shapes.parquet']   # the map reads these
DATASET_FILES = BUCKET_FILES + ['release-notes.txt'] + [f'gtfs/{t}.parquet' for t in TABLES]
KAGGLE = ('helenkwok/sa-transit', 'Milleleaf: South Australia transit')        # mirrored by kaggle_mirror.py (title 6-50 chars)

TIMETABLE = ['trips', 'stop_times', 'calendar', 'calendar_dates']

def comparable(new, old, old_valid):
    """the (new, old) manifests manifest.problems compares: each TIMETABLE count swapped for its <t>_valid count on both sides;
    old_valid: the published feed's <t>_valid counts at this bake date, or None. Returns them and the tables left on the raw rule."""
    if not old: return new, old, []
    nc, oc, raw = dict(new['counts']), dict(old['counts']), []
    for t in TIMETABLE:
        k = t + '_valid'; v = (old_valid or {}).get(k, oc.get(k))     # oc[k]: counted at the published bake's own date (fallback)
        if v is None or k not in nc: raw.append(t); oc.pop(k, None)
        else: oc[k] = v; oc.pop(t, None); nc.pop(t, None)
    return {**new, 'counts': nc}, {**old, 'counts': oc}, raw

def published_valid(day):
    base = f'https://huggingface.co/datasets/{REPO}/resolve/main/gtfs/'
    try:
        con = duckdb.connect(); con.execute('INSTALL httpfs; LOAD httpfs')
        return bake.valid_counts(con, lambda t: f"read_parquet('{base}{t}.parquet')", day)
    except Exception as e:
        print(f'publish gate: published gtfs not readable ({e})', file=sys.stderr); return None

def gate(new, url, day):
    old = manifest.published(url)
    n, o, raw = comparable(new, old, published_valid(day) if old else None)
    for t in raw: print(f'publish gate: {t}: no valid-service count for the published build, raw comparison this run', file=sys.stderr)
    for k, v in o['counts'].items() if o else ():
        if k.endswith('_valid'): print(f'publish gate: {k} {v} -> {n["counts"].get(k)} (services running on or after {day})', file=sys.stderr)
    p = manifest.problems(n, o)
    if p and os.environ.get('PUBLISH_FORCE') != '1':
        sys.exit('publish gate: not publishing (PUBLISH_FORCE=1 overrides):\n  ' + '\n  '.join(p))
    for x in p: print(f'publish gate: forced past: {x}', file=sys.stderr)

def test():
    con = duckdb.connect()                                             # services: a ended 20261001, b runs on, c added 20261025
    con.execute("CREATE TABLE calendar AS SELECT * FROM (VALUES ('a', '20261001'), ('b', '20261231')) v(service_id, end_date)")
    con.execute("CREATE TABLE calendar_dates AS SELECT * FROM (VALUES ('c', '20261025', '1'), ('b', '20261030', '2')) v(service_id, date, exception_type)")
    con.execute("CREATE TABLE trips AS SELECT * FROM (VALUES ('t1', 'a'), ('t2', 'b'), ('t3', 'c')) v(trip_id, service_id)")
    con.execute("CREATE TABLE stop_times AS SELECT * FROM (VALUES ('t1'), ('t1'), ('t2'), ('t3')) v(trip_id)")
    assert bake.valid_counts(con, lambda t: t, '20261023') == {'trips_valid': 2, 'calendar_valid': 1, 'calendar_dates_valid': 2, 'stop_times_valid': 2}
    f = lambda trips, valid, stops=100: {'files': {'a': {'bytes': 1}}, 'counts': {'trips': trips, 'trips_valid': valid, 'stops': stops}}
    old = {'files': {'a': {'bytes': 1}}, 'counts': {'trips': 30293, 'stops': 100}}          # published before *_valid existed
    pv = {'trips_valid': 20661}                                                              # recounted from the published gtfs
    assert manifest.problems(*comparable(f(21691, 21691), old, pv)[:2]) == []                # only expired services went: passes
    assert manifest.problems(*comparable(f(21691, 18000), old, pv)[:2])                      # valid services dropped 13%: refused
    assert manifest.problems(*comparable(f(21691, 21691, 80), old, pv)[:2])                  # other counts keep the raw rule
    assert manifest.problems(*comparable(f(21691, 21691), old, None)[:2])                    # nothing to recount: raw rule, refused
    assert comparable(f(21691, 21691), old, None)[2] == TIMETABLE
    assert manifest.problems(*comparable(f(21691, 21691), f(30293, 21000), None)[:2]) == []  # the published manifest's *_valid
    print('publish: self-test ok')

def main():
    o = lambda f: os.path.join(sources.OUT, f); idx = json.load(open(o('index.json')))
    m = manifest.manifest(sources.OUT, sorted(set(DATASET_FILES + BUCKET_FILES)), idx['counts'], idx['built'])
    gate(m, f'https://huggingface.co/buckets/{REPO}/resolve/manifest.json', idx['built'][:10].replace('-', ''))
    json.dump(m, open(o('manifest.json'), 'w'), indent=1)
    api = HfApi(); api.create_repo(REPO, repo_type='dataset', exist_ok=True)
    card = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hf', 'README.md')
    api.create_commit(REPO, repo_type='dataset', commit_message=f"bake of {idx['built']} (feed {(idx.get('feed_info') or {}).get('feed_version')})",
                      operations=[CommitOperationAdd(f, o(f)) for f in DATASET_FILES + ['manifest.json']] + [CommitOperationAdd('README.md', card)])
    create_bucket(REPO, private=False, exist_ok=True)
    batch_bucket_files(REPO, add=[(o(f), f) for f in BUCKET_FILES] + [(o('manifest.json'), 'manifest.json')])   # the gate reads the bucket's
    print(json.dumps({'dataset': f'https://huggingface.co/datasets/{REPO}', 'bucket': f'https://huggingface.co/buckets/{REPO}', 'built': idx['built'], 'counts': m['counts']}))

if __name__ == '__main__':
    test() if sys.argv[1:] == ['test'] else main()
