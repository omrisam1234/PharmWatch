# sp_pipeline.py
import argparse, subprocess, sys, shlex, datetime, os
from pathlib import Path

def run(cmd):
    print("→", cmd)
    res = subprocess.run(shlex.split(cmd), check=True)
    return res.returncode

def main():
    ap = argparse.ArgumentParser(description="Daily Super-Pharm pipeline")
    ap.add_argument("--branch", required=True)                 # e.g. "סופר-פארם קרית אתא"
    ap.add_argument("--store-id", required=True)               # e.g. 072
    ap.add_argument("--date", default=None)                    # YYYY-MM-DD; default=today
    ap.add_argument("--limit", type=int, default=200)          # how many rows to scan
    ap.add_argument("--out", default="./sp_data")
    ap.add_argument("--db", default=os.environ.get("SPHARM_DB", "superpharm.db"))
    args = ap.parse_args()

    date = args.date or datetime.date.today().isoformat()
    out = Path(args.out); (out / "raw").mkdir(parents=True, exist_ok=True); (out / "csv").mkdir(parents=True, exist_ok=True)

    # 1) Fetch & parse (PriceFull + PromoFull)
    run(f'python sp_fetch_and_parse.py --branch "{args.branch}" --category PriceFull --date {date} --limit {args.limit} --out {args.out}')
    run(f'python sp_fetch_and_parse.py --branch "{args.branch}" --category PromoFull --date {date} --limit {args.limit} --out {args.out}')

    # 2) Merge into one canonical CSV for the day/store
    merged_csv = out / f"superpharm_prices_with_promos.{args.store_id}.{date}.csv"
    run(f'python sp_merge_day.py --csv-dir {args.out}/csv --store-id {args.store-id if False else args.store_id} --date {date} --out "{merged_csv}"')

    # 3) Load into SQLite
    run(f'python load_sqlite.py --db {args.db} --csv "{merged_csv}" --store-id {args.store_id} --observed-at "{date} 07:00"')

    print("✓ Pipeline done:", date, "store:", args.store_id, "db:", args.db)

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)
