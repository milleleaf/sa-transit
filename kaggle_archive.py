"""The live archive: every complete UTC month of each live.py feed as its own public Kaggle dataset, created once and never
versioned or deleted (Kaggle public storage is free; Hugging Face keeps only the current month, see hf/README.md).
  helenkwok/sa-transit-vehicles-YYYY-MM       live/vehicles-YYYY-MM-DD.parquet for every day of that month on the bucket
  helenkwok/sa-transit-trip-updates-YYYY-MM   live/trip_updates-YYYY-MM-DD.parquet
  helenkwok/sa-transit-alerts-YYYY-MM         live/alerts-YYYY-MM-DD.parquet
A month's day files leave the bucket only after `kaggle datasets files` lists every one of them. Runs in the weekly bake
(its own step, allowed to fail), so a month is archived within a week of ending.
Needs HF_TOKEN and KAGGLE_API_TOKEN. Usage: python3 kaggle_archive.py      (self-test, no network: python3 kaggle_archive.py test)"""
import datetime, json, os, re, subprocess, sys, tempfile, time
from huggingface_hub import batch_bucket_files, download_bucket_files, list_bucket_tree
import kaggle_mirror

REPO = 'milleleaf/sa-transit'
DAY = re.compile(r'live/(vehicles|trip_updates|alerts)-(\d{4}-\d{2})-\d{2}\.parquet')
TITLE = {'vehicles': 'vehicle positions', 'trip_updates': 'trip updates', 'alerts': 'service alerts'}
slug = lambda feed, month: f"helenkwok/sa-transit-{feed.replace('_', '-')}-{month}"


def past_months(paths, now):
    """{(feed, month): [day paths]} for every month before now's UTC month"""
    this, out = now.strftime('%Y-%m'), {}
    for p in sorted(paths):
        if (m := DAY.fullmatch(p)) and m.group(2) < this: out.setdefault((m.group(1), m.group(2)), []).append(p)
    return out


def listed(s, paths):
    r = subprocess.run(['kaggle', 'datasets', 'files', s, '-v', '--page-size', '200'], capture_output=True, text=True)
    return r.returncode == 0 and all(os.path.basename(p) in r.stdout for p in paths)


def main():
    months = past_months([f.path for f in list_bucket_tree(REPO, prefix='live', recursive=True)], datetime.datetime.now(datetime.UTC))
    done = []
    for (feed, month), paths in months.items():
        s = slug(feed, month)
        if not listed(s, paths):
            if subprocess.run(['kaggle', 'datasets', 'files', s], capture_output=True).returncode == 0:
                print(f'::warning::{s} exists but does not list all {len(paths)} days: kept on the bucket, check by hand'); continue
            with tempfile.TemporaryDirectory() as root:
                download_bucket_files(REPO, [(p, os.path.join(root, p)) for p in paths])
                kaggle_mirror.mirror(s, f'Milleleaf: SA {TITLE[feed]} {month}', root, paths)
            for _ in range(30):                                        # Kaggle processes an upload before listing its files
                if listed(s, paths): break
                time.sleep(60)
            else:
                print(f'::warning::{s}: not fully listed after 30 minutes; kept on the bucket'); continue
        batch_bucket_files(REPO, delete=paths)
        done.append({'feed': feed, 'month': month, 'dataset': f'https://www.kaggle.com/datasets/{s}', 'days': len(paths)})
    print(json.dumps({'archived': done}))


if __name__ == '__main__':
    if sys.argv[1:] == ['test']:
        p = ['live/vehicles.json', 'live/vehicles-2026-09-30.parquet', 'live/vehicles-2026-10-01.parquet', 'live/vehicles-2025-12-31.parquet',
             'live/trip_updates-2026-09-01.parquet', 'live/alerts-2026-09-02.parquet', 'live/alerts.json']
        assert past_months(p, datetime.datetime(2026, 10, 5)) == {('vehicles', '2025-12'): [p[3]], ('vehicles', '2026-09'): [p[1]],
                                                                  ('trip_updates', '2026-09'): [p[4]], ('alerts', '2026-09'): [p[5]]}
        assert slug('trip_updates', '2026-10') == 'helenkwok/sa-transit-trip-updates-2026-10'
        assert all(6 <= len(f'Milleleaf: SA {t} 2026-10') <= 50 for t in TITLE.values())
        print('kaggle_archive: self-test ok')
    else:
        main()
