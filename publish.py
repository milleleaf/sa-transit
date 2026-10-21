"""Publish sources.OUT to Hugging Face (needs HF_TOKEN with write access to the alreadyopen org):
  dataset  REPO  DATASET_FILES + manifest.json + the card (hf/README.md): one commit a run, versioned (data tools)
  bucket   REPO  BUCKET_FILES + manifest.json, overwritten in place (read by browsers: CORS + HTTP Range); live.py adds live/*
Nothing is uploaded unless the publish gate passes (manifest.py).
Usage: python3 publish.py     (after bake.py)"""
import json, os
from huggingface_hub import CommitOperationAdd, HfApi, batch_bucket_files, create_bucket
import manifest, sources

REPO = 'milleleaf/sa-transit'
_idx = os.path.join(sources.OUT, 'index.json')
TABLES = json.load(open(_idx))['tables'] if os.path.exists(_idx) else []
BUCKET_FILES = ['index.json', 'transit.pmtiles', 'stops.json', 'routes.json', 'stops.parquet', 'route-shapes.parquet']   # the map reads these
DATASET_FILES = BUCKET_FILES + ['release-notes.txt'] + [f'gtfs/{t}.parquet' for t in TABLES]
KAGGLE = ('helenkwok/sa-transit', 'AlreadyOpen: South Australia transit')        # mirrored by kaggle_mirror.py (title 6-50 chars)

def main():
    o = lambda f: os.path.join(sources.OUT, f); idx = json.load(open(o('index.json')))
    m = manifest.manifest(sources.OUT, sorted(set(DATASET_FILES + BUCKET_FILES)), idx['counts'], idx['built'])
    manifest.gate(m, f'https://huggingface.co/buckets/{REPO}/resolve/manifest.json')
    json.dump(m, open(o('manifest.json'), 'w'), indent=1)
    api = HfApi(); api.create_repo(REPO, repo_type='dataset', exist_ok=True)
    card = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hf', 'README.md')
    api.create_commit(REPO, repo_type='dataset', commit_message=f"bake of {idx['built']} (feed {(idx.get('feed_info') or {}).get('feed_version')})",
                      operations=[CommitOperationAdd(f, o(f)) for f in DATASET_FILES + ['manifest.json']] + [CommitOperationAdd('README.md', card)])
    create_bucket(REPO, private=False, exist_ok=True)
    batch_bucket_files(REPO, add=[(o(f), f) for f in BUCKET_FILES] + [(o('manifest.json'), 'manifest.json')])   # the gate reads the bucket's
    print(json.dumps({'dataset': f'https://huggingface.co/datasets/{REPO}', 'bucket': f'https://huggingface.co/buckets/{REPO}', 'built': idx['built'], 'counts': m['counts']}))

if __name__ == '__main__':
    main()
