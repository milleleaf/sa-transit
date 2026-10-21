# sa-transit

South Australia public transport from the [Adelaide Metro GTFS feed (CC BY 4.0)](https://data.sa.gov.au/data/dataset/4e191c1e-b971-441f-83f7-45e266c41b99)
(metro buses, trains, trams and the regional coaches in the same feed), as GeoParquet, JSON and vector tiles, published weekly
to Hugging Face ([dataset](https://huggingface.co/datasets/milleleaf/sa-transit),
[bucket](https://huggingface.co/buckets/milleleaf/sa-transit) read by browsers) and mirrored to
[Kaggle](https://www.kaggle.com/datasets/helenkwok/sa-transit). The GTFS-realtime vehicle positions, trip updates and service alerts
are snapshotted every 15 minutes to the bucket (`live/`), and each complete month is archived on Kaggle
(`helenkwok/sa-transit-{vehicles,trip-updates,alerts}-YYYY-MM`). Built from
[pipeline-template](https://github.com/milleleaf/pipeline-template). Every table, row and field is kept; the files are
listed in `hf/README.md`.

    python3 bake.py            # data/out/: Parquet, JSON, PMTiles + index.json (needs tippecanoe 2.79)
    python3 publish.py         # gate, then Hugging Face (HF_TOKEN)
    python3 live.py            # one snapshot of each realtime feed to the bucket (HF_TOKEN), every 15 min in live.yml
    python3 kaggle_archive.py  # complete months of snapshots to Kaggle, then off the bucket (weekly, in bake.yml)
    python3 check.py           # data-free checks (CI)

The bake stops if a tile layer's deepest zoom holds fewer features than there are rows, or a shape is used by no trip.

Code: Apache-2.0. Data: CC BY 4.0 (Government of South Australia), see `sources.json`.
