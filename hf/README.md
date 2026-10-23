---
pretty_name: South Australia public transport (Adelaide Metro GTFS)
license: cc-by-4.0
license_link: https://creativecommons.org/licenses/by/4.0/
tags:
  - geospatial
  - transit
  - gtfs
  - gtfs-realtime
  - adelaide
  - south-australia
---

# South Australia public transport

The Adelaide Metro General Transit Feed (buses, trains and trams in metropolitan Adelaide, plus the regional coach operators
the same feed carries, such as Stateliner) as GeoParquet, JSON and vector tiles, rebuilt weekly by
[milleleaf/sa-transit](https://github.com/milleleaf/sa-transit), with a snapshot of the GTFS-realtime feeds (vehicle
positions, trip updates and service alerts) every 15 minutes. Every table, row and field of the source is kept.

| File | Holds |
|---|---|
| `gtfs/<table>.parquet` | Every table of the feed (`agency`, `routes`, `trips`, `stop_times`, `stops`, `shapes`, `calendar`, `calendar_dates`, `transfers`, `booking_rules`, `feed_info`, and any table added later), every column as text, exactly as published |
| `release-notes.txt` | The feed's own release notes |
| `stops.parquet` | GeoParquet: every stop, every column, with a point geometry |
| `route-shapes.parquet` | GeoParquet: one row per route and shape a trip uses: every route column, `shape_id`, `trips`, `points`, `shape_dist_traveled`, and the shape as a line |
| `stops.json` | GeoJSON of every stop, every field |
| `routes.json` | Every route, every field, with its `shape_ids` and `trips` |
| `transit.pmtiles` | Vector tiles, layers `routes` and `stops`, zooms 4 to 14; every feature is in every zoom |
| `index.json` | Row counts, the tables, and the feed's `feed_info` (version and validity dates) |

## Live feeds (bucket only)

On the [bucket](https://huggingface.co/buckets/milleleaf/sa-transit), refreshed every 15 minutes (GitHub may start a run
late or skip one, so the history has gaps). Three GTFS-realtime feeds, each as `<feed>`:

| `<feed>` | Feed | Daily Parquet: one row per |
|---|---|---|
| `vehicles` | vehicle positions | vehicle per snapshot (GeoParquet, a point per vehicle) |
| `trip_updates` | trip updates (predicted arrivals and departures) | trip per snapshot; `stop_time_updates` counts its stop predictions, which are in `entity` |
| `alerts` | service alerts (disruptions, stop changes) | alert per snapshot |

| File | Holds |
|---|---|
| `live/<feed>.json` | The latest feed decoded to JSON, every field, including the feed's own vehicle extension (`air_conditioned`, `wheelchair_accessible` as `tfnsw_vehicle_descriptor` on every vehicle descriptor) |
| `live/<feed>.pb` | The same feed as received (protobuf) |
| `live/<feed>-YYYY-MM-DD.parquet` | Every entity in every snapshot of that UTC day, with the main fields as columns and `entity`, the full entity as JSON |

`snapshot` is the feed's own header timestamp, kept as published. The service alerts feed's header is Adelaide local time
written as if it were UTC (10.5 hours ahead during daylight saving; seen 2026-10-11 and measured again 2026-10-23), while the
other two feeds are in true UTC. So since 2026-10-23 every row also has `ingested_at`, the UTC time this pipeline fetched the
feed, and the day file is taken from `ingested_at`: **use `ingested_at` for alerts.** Earlier alert files were named by the
shifted header, so an alert day file before 2026-10-23 can hold the evening of the day before.

The bucket keeps the current month (plus the previous one until it is archived). **The full history is on Kaggle**, one
dataset per feed and month, never changed once created: `helenkwok/sa-transit-vehicles-YYYY-MM`,
`helenkwok/sa-transit-trip-updates-YYYY-MM` and `helenkwok/sa-transit-alerts-YYYY-MM`
(e.g. https://www.kaggle.com/datasets/helenkwok/sa-transit-vehicles-2026-10).

`manifest.json` lists every file's sha256, the counts and the sources of the latest bake. Coordinates are WGS 84 as
published in the feed.

## Sources and terms

Data from the Department for Infrastructure and Transport (Adelaide Metro), Government of South Australia:
[Adelaide Metro General Transit Feed](https://data.sa.gov.au/data/dataset/4e191c1e-b971-441f-83f7-45e266c41b99) and its
[GTFS-realtime API](https://gtfs.adelaidemetro.com.au/), licensed under
[Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/). Changes: converted to Parquet,
GeoParquet, JSON and vector tiles, with shapes joined to routes; nothing removed. Provided as is, with no warranty, not
endorsed by Adelaide Metro or the Government of South Australia. Do not rely on it for travel; use Adelaide Metro's own
services.
