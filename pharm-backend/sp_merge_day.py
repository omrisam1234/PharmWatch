#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge one day's PriceFull + PromoFull CSVs into a single canonical CSV.

Output columns:
barcode, name, price, quantity, qty_unit, computed_unit_price, computed_unit_label,
has_promo, PromotionIds, RewardTypes, AnyGift, AnyMulti,
manufacturer, country, description, price_update_date

Usage:
  python sp_merge_day.py --csv-dir ./sp_data/csv --store-id 072 --date 2025-08-28 \
    --out ./sp_data/superpharm_prices_with_promos.072.2025-08-28.csv
"""
import argparse, glob, re
import pandas as pd
from pathlib import Path

def choose(df, prefer, *alts):
    if prefer in df.columns: return prefer
    for c in alts:
        if c in df.columns: return c
    raise KeyError(f"Missing column: {prefer} / {alts}")

def unit_kind(text):
    t = (str(text) if text is not None else "").strip()
    if t in ("גרם","ג'", "ג", "גר"): return "g"
    if 'מ"ל' in t or 'מ״ל' in t or 'מ”ל' in t or t=='מל' or 'מיליליטר' in t: return "ml"
    if t in ("יחידה","יח'", "יחידות", "unit"): return "unit"
    return None

def to_float(x):
    if x is None: return None
    s = str(x).replace(",", ".").strip()
    try: return float(s)
    except: return None

def per100(price, qty, kind):
    if price is None or qty is None or qty <= 0: return None, None
    if kind == "g":  return round(price / (qty/100.0), 2), "per_100g"
    if kind == "ml": return round(price / (qty/100.0), 2), "per_100ml"
    return None, None

def find_latest(csv_dir: Path, kind: str, store_id: str, date_yyyymmdd: str):
    # files are like: PriceFull<chain>-<store>-YYYYMMDDhhmm.csv
    pats = [
        f"{kind}*-{store_id}-*.csv",
        f"{kind}*{store_id}*.csv",
    ]
    cands = []
    for p in pats: cands += sorted(csv_dir.glob(p))
    # filter by date if present in filename
    if date_yyyymmdd:
        cands = [p for p in cands if date_yyyymmdd in p.name]
    if not cands:
        raise FileNotFoundError(f"No {kind} CSVs found for store {store_id} in {csv_dir}")
    return cands[-1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-dir", default="./sp_data/csv")
    ap.add_argument("--store-id", required=True)
    ap.add_argument("--date", required=False, help="YYYY-MM-DD; optional")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    csv_dir = Path(args.csv_dir)
    date_yyyymmdd = args.date.replace("-", "") if args.date else ""

    price_csv = find_latest(csv_dir, "PriceFull", args.store_id, date_yyyymmdd)
    promo_csv = find_latest(csv_dir, "PromoFull", args.store_id, date_yyyymmdd)

    prices = pd.read_csv(price_csv, dtype=str, keep_default_na=False)
    promos  = pd.read_csv(promo_csv, dtype=str, keep_default_na=False)

    # --- normalize prices
    barcode_col = choose(prices, "ItemCode", "barcode")
    name_col    = choose(prices, "ItemName", "name")
    price_col   = choose(prices, "ItemPrice", "price")
    qty_col     = choose(prices, "Quantity", "quantity")
    unitqty_col = "UnitQty" if "UnitQty" in prices.columns else ( "unit_qty_text" if "unit_qty_text" in prices.columns else None )

    dfp = pd.DataFrame({
        "barcode": prices[barcode_col].astype(str).str.strip(),
        "name": prices[name_col],
        "price": prices[price_col].apply(to_float),
        "quantity": prices[qty_col].apply(to_float),
        "unit_qty_text": prices[unitqty_col] if unitqty_col else "",
        "manufacturer": prices.get("ManufacturerName",""),
        "country": prices.get("ManufactureCountry",""),
        "description": prices.get("ManufacturerItemDescription",""),
        "price_update_date": prices.get("PriceUpdateDate",""),
    })
    dfp["qty_unit"] = dfp["unit_qty_text"].apply(unit_kind)
    per, label = zip(*dfp.apply(lambda r: per100(r["price"], r["quantity"], r["qty_unit"]), axis=1))
    dfp["computed_unit_price"]  = list(per)
    dfp["computed_unit_label"]  = list(label)

    # --- normalize promos
    promo_itemcode_col = choose(promos, "ItemCode", "barcode")
    dpp = promos[[promo_itemcode_col]].copy()
    dpp["PromotionId"] = promos.get("PromotionId","")
    dpp["RewardType"]  = promos.get("RewardType","")
    dpp["IsGiftItem"]  = promos.get("IsGiftItem","")
    dpp["AllowMultipleDiscounts"] = promos.get("AllowMultipleDiscounts","")

    agg = (dpp.groupby(promo_itemcode_col, dropna=False)
                .agg(PromotionIds=("PromotionId", lambda s: ",".join(sorted({v for v in s if v})) ),
                     RewardTypes=("RewardType", lambda s: ",".join(sorted({v for v in s if v})) ),
                     AnyGift=("IsGiftItem", lambda s: "1" if (s=="1").any() else "0"),
                     AnyMulti=("AllowMultipleDiscounts", lambda s: "1" if (s=="1").any() else "0"))
                .reset_index()
                .rename(columns={promo_itemcode_col:"barcode"}))
    agg["barcode"] = agg["barcode"].astype(str).str.strip()

    merged = dfp.merge(agg, on="barcode", how="left")
    merged["has_promo"] = merged["PromotionIds"].fillna("").ne("")

    cols = ["barcode","name","price","quantity","qty_unit","computed_unit_price","computed_unit_label",
            "has_promo","PromotionIds","RewardTypes","AnyGift","AnyMulti",
            "manufacturer","country","description","price_update_date"]
    for c in cols:
        if c not in merged.columns: merged[c] = ""
    merged = merged[cols].sort_values(["name","barcode"]).reset_index(drop=True)

    outp = Path(args.out); outp.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(outp, index=False, encoding="utf-8")
    print(f"[ok] wrote {outp}  rows={len(merged)}  promos={merged['has_promo'].sum()}")
    print(f"[src] prices: {price_csv.name}")
    print(f"[src] promos: {promo_csv.name}")

if __name__ == "__main__":
    main()
