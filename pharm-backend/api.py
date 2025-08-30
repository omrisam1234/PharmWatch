# api.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3, os
from typing import List, Optional, Dict, Any
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.environ.get("SPHARM_DB", "superpharm.db")

app = FastAPI(title="SuperPharm Prices API", version="0.1")


# Allow local dev frontends (adjust for your domain/ports)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _db_has_schema(conn: sqlite3.Connection) -> bool:
    """
    Returns True if the 'products' table exists; otherwise False.
    Uses sqlite_schema to avoid raising, fast and safe preflight.
    """
    try:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='products' LIMIT 1"
        )
        return cur.fetchone() is not None
    except sqlite3.Error:
        return False


def row_to_dict(cur, row):
    return {d[0]: row[i] for i, d in enumerate(cur.description)}

def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = row_to_dict
    return con

@app.get("/health")
def health():
    if not os.path.exists(DB_PATH):
        return {"ok": False, "error": f"DB not found at {DB_PATH}"}
    return {"ok": True}

@app.get("/search")
def search(q: str = Query("", min_length=0), limit: int = 50, store_id: Optional[str] = None):
    con = connect()
    try:
        if not _db_has_schema(con):
            raise HTTPException(status_code=503, detail="Database not initialized yet")

        cur = con.cursor()
        like = f"%{q}%"
        params: list[Any] = []
        sql = """
          SELECT p.barcode, p.name,
                 cp.price_agorot/100.0 AS price,
                 cp.unit_price_per100_agorot/100.0 AS unit_price_per100,
                 cp.qty_unit,
                 cp.has_promo, cp.reward_types,
                 cp.last_seen_at
          FROM products p
          JOIN current_prices cp ON p.barcode = cp.barcode
          WHERE p.name LIKE ?
        """
        params.append(like)
        if store_id:
            sql += " AND cp.store_id = ?"
            params.append(store_id)
        sql += " ORDER BY COALESCE(cp.unit_price_per100_agorot, cp.price_agorot) ASC, p.name ASC LIMIT ?"
        params.append(int(limit))
        cur.execute(sql, params)
        out = cur.fetchall()
        return {"results": out, "count": len(out)}
    finally:
        con.close()


@app.get("/item/{barcode}")
def item_detail(barcode: str, store_id: Optional[str] = None):
    con = connect()
    try:
        if not _db_has_schema(con):
            raise HTTPException(status_code=503, detail="Database not initialized yet")

        cur = con.cursor()
        cur.execute("SELECT * FROM products WHERE barcode = ?", (barcode,))
        prod = cur.fetchone()
        if not prod:
            raise HTTPException(status_code=404, detail="barcode not found")

        if store_id:
            cur.execute("""
              SELECT store_id, price_agorot/100.0 AS price, quantity, qty_unit,
                     unit_price_per100_agorot/100.0 AS unit_price_per100,
                     has_promo, reward_types, last_seen_at
              FROM current_prices WHERE barcode = ? AND store_id = ?
            """, (barcode, store_id))
        else:
            cur.execute("""
              SELECT store_id, price_agorot/100.0 AS price, quantity, qty_unit,
                     unit_price_per100_agorot/100.0 AS unit_price_per100,
                     has_promo, reward_types, last_seen_at
              FROM current_prices WHERE barcode = ?
              ORDER BY price_agorot ASC LIMIT 1
            """, (barcode,))
        price_row = cur.fetchone()
        return {"product": prod, "current_price": price_row}
    finally:
        con.close()


@app.get("/item/{barcode}/history")
def item_history(barcode: str, store_id: Optional[str] = None, days: int = 30):
    con = connect()
    try:
        if not _db_has_schema(con):
            raise HTTPException(status_code=503, detail="Database not initialized yet")

        cur = con.cursor()
        if store_id:
            cur.execute("""
              SELECT observed_at, price_agorot/100.0 AS price,
                     unit_price_per100_agorot/100.0 AS unit_price_per100,
                     has_promo, reward_types
              FROM price_observations
              WHERE barcode = ? AND store_id = ?
                AND observed_at >= datetime('now', ?)
              ORDER BY observed_at ASC
            """, (barcode, store_id, f"-{int(days)} days"))
        else:
            cur.execute("""
              SELECT store_id, observed_at, price_agorot/100.0 AS price,
                     unit_price_per100_agorot/100.0 AS unit_price_per100,
                     has_promo, reward_types
              FROM price_observations
              WHERE barcode = ?
                AND observed_at >= datetime('now', ?)
              ORDER BY observed_at ASC
            """, (barcode, f"-{int(days)} days"))
        rows = cur.fetchall()
        return {"history": rows, "count": len(rows)}
    finally:
        con.close()