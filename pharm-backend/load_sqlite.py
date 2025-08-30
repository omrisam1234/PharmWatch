# load_sqlite.py
import argparse, csv, sqlite3, math, os
from pathlib import Path
from datetime import datetime

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS stores(
  store_id TEXT PRIMARY KEY,
  chain_id TEXT,
  name TEXT,
  city TEXT,
  address TEXT,
  last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS products(
  barcode TEXT PRIMARY KEY,
  name TEXT,
  manufacturer TEXT,
  country TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS current_prices(
  store_id TEXT,
  barcode TEXT,
  price_agorot INTEGER,
  quantity REAL,
  qty_unit TEXT,
  unit_price_per100_agorot INTEGER,
  has_promo INTEGER,
  reward_types TEXT,
  last_seen_at TEXT,
  PRIMARY KEY(store_id, barcode),
  FOREIGN KEY(barcode) REFERENCES products(barcode),
  FOREIGN KEY(store_id) REFERENCES stores(store_id)
);

CREATE TABLE IF NOT EXISTS price_observations(
  store_id TEXT,
  barcode TEXT,
  observed_at TEXT,
  price_agorot INTEGER,
  unit_price_per100_agorot INTEGER,
  has_promo INTEGER,
  reward_types TEXT,
  source_file TEXT,
  PRIMARY KEY(store_id, barcode, observed_at)
);

CREATE TABLE IF NOT EXISTS files_ingested(
  store_id TEXT,
  kind TEXT CHECK (kind IN ('prices','promos','merged')),
  published_ts TEXT,
  sha256 TEXT,
  PRIMARY KEY(store_id, kind, published_ts)
);

CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
CREATE INDEX IF NOT EXISTS idx_obs_lookup ON price_observations(store_id, barcode, observed_at);
"""

def agorot(x: str | float | None) -> int | None:
    if x in (None, "", "None"): return None
    try:
        v = float(str(x).replace(",", "."))
    except ValueError:
        return None
    return int(round(v * 100))

def booly(x) -> int:
    # accept True/False, 1/0, "true"/"false"
    s = str(x).strip().lower()
    return 1 if s in ("1","true","yes","y","t") else 0

def parse_args():
    ap = argparse.ArgumentParser(description="Load Super-Pharm merged CSV into SQLite")
    ap.add_argument("--db", required=True, help="Path to SQLite DB, e.g., superpharm.db")
    ap.add_argument("--csv", required=True, help="Path to superpharm_prices_with_promos.csv")
    ap.add_argument("--store-id", required=True, help="Store id (e.g., 072)")
    ap.add_argument("--observed-at", default=None,
                    help='Timestamp (e.g., "2025-08-28 07:06"). Default = now()')
    ap.add_argument("--chain-id", default="7290172900007")
    ap.add_argument("--store-name", default=None)
    ap.add_argument("--store-city", default=None)
    ap.add_argument("--store-address", default=None)
    return ap.parse_args()

def norm(s):
    # Trim; return None if empty so COALESCE keeps existing DB value
    v = (s or "").strip()
    return v if v else None

def main():
    args = parse_args()
    observed_at = args.observed_at or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    dbp = Path(args.db)
    dbp.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(dbp))
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA_SQL)

    # Upsert the store (minimal metadata)
    conn.execute("""
        INSERT INTO stores(store_id, chain_id, name, city, address, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(store_id) DO UPDATE SET
          chain_id=excluded.chain_id,
          name=COALESCE(excluded.name, stores.name),
          city=COALESCE(excluded.city, stores.city),
          address=COALESCE(excluded.address, stores.address),
          last_seen_at=excluded.last_seen_at
    """, (args.store_id, args.chain_id, args.store_name, args.store_city, args.store_address, observed_at))

    # Read your merged CSV
    rows_read = 0
    inserted_products = 0
    upsert_current = 0
    appended_obs = 0

    with open(args.csv, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            rows_read += 1



            barcode      = (row.get("barcode") or "").strip()

            # SWAP: products.name should get LONG text (CSV "description"),
            #       products.description should get SHORT label (CSV "name")
            name_long    = norm(row.get("description"))   # long text
            desc_short   = norm(row.get("name"))          # short label

            manufacturer = norm(row.get("manufacturer"))
            country      = norm(row.get("country"))

            price_a  = agorot(row.get("price"))
            # quantity / units as you had them:
            try:
                qty = float(row.get("quantity") or "") if row.get("quantity") not in ("", None) else None
            except ValueError:
                qty = None
            qty_unit = row.get("qty_unit") or None
            per100_a = agorot(row.get("computed_unit_price"))
            has_promo = booly(row.get("has_promo"))
            reward_types = row.get("RewardTypes") or row.get("reward_types") or ""

            if not barcode or price_a is None:
                # Skip rows without essential fields
                continue

            # products upsert (we keep name/manufacturer/etc. here)
            conn.execute("""
                INSERT INTO products(barcode, name, manufacturer, country, description)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(barcode) DO UPDATE SET
                  name=COALESCE(excluded.name, products.name),
                  manufacturer=COALESCE(excluded.manufacturer, products.manufacturer),
                  country=COALESCE(excluded.country, products.country),
                  description=COALESCE(excluded.description, products.description)
            """, (barcode, name_long, manufacturer, country, desc_short))
            inserted_products += 1

            # current_prices upsert
            conn.execute("""
                INSERT INTO current_prices(
                  store_id, barcode, price_agorot, quantity, qty_unit,
                  unit_price_per100_agorot, has_promo, reward_types, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(store_id, barcode) DO UPDATE SET
                  price_agorot=excluded.price_agorot,
                  quantity=excluded.quantity,
                  qty_unit=excluded.qty_unit,
                  unit_price_per100_agorot=excluded.unit_price_per100_agorot,
                  has_promo=excluded.has_promo,
                  reward_types=excluded.reward_types,
                  last_seen_at=excluded.last_seen_at
            """, (args.store_id, barcode, price_a, qty, qty_unit, per100_a, has_promo, reward_types, observed_at))
            upsert_current += 1

            # history append (simple mode: always insert; skip if duplicate key)
            try:
                conn.execute("""
                    INSERT INTO price_observations(
                      store_id, barcode, observed_at, price_agorot, unit_price_per100_agorot,
                      has_promo, reward_types, source_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (args.store_id, barcode, observed_at, price_a, per100_a, has_promo, reward_types, os.path.basename(args.csv)))
                appended_obs += 1
            except sqlite3.IntegrityError:
                # already inserted for this (store, barcode, observed_at)
                pass

    conn.commit()
    conn.close()

    print(f"Rows read: {rows_read}")
    print(f"Products upserted: {inserted_products}")
    print(f"current_prices upserted: {upsert_current}")
    print(f"price_observations appended: {appended_obs}")
    print(f"DB: {args.db} updated; store_id={args.store_id}, observed_at={observed_at}")
if __name__ == "__main__":
    main()
