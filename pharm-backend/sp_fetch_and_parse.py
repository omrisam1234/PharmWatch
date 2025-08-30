#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List -> download -> parse Super-Pharm XML (.gz) into normalized CSV.
Supports: PriceFull, PromoFull (others pass through as line dumps).

Usage examples:
  python sp_fetch_and_parse.py \
    --branch "סופר-פארם קרית אתא" --date 2025-08-28 \
    --category PriceFull --limit 20 --out ./sp_data

  python sp_fetch_and_parse.py \
    --branch "סופר-פארם קרית אתא" --date 2025-08-28 \
    --category PromoFull --limit 20 --out ./sp_data
"""
import argparse, csv, gzip, io, os, re, sys, urllib.parse, xml.etree.ElementTree as ET
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path

BASE_URL = "https://prices.super-pharm.co.il/"

def to_portal_date(s: str) -> str:
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return s  # assume already dd/MM/yyyy

# ---------- portal listing (server-rendered mvc-grid) ----------
def fetch_page(session: requests.Session, params: dict) -> tuple[str,str]:
    r = session.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.text, r.url

def parse_table(html: str, base_for_links: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.mvc-grid") or soup.find("table")
    if not table:
        return []
    body = table.find("tbody") or table
    out = []
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds: continue
        href = None
        for td in tds:
            a = td.find("a", href=True)
            if a and (a["href"].endswith(".gz") or "/Download/" in a["href"]):
                href = urllib.parse.urljoin(base_for_links, a["href"]); break
        def txt(i): return tds[i].get_text(strip=True) if i < len(tds) else ""
        out.append({
            "href": href,
            "branch": txt(1),
            "category": txt(2),
            "date": txt(3),
            "name": txt(4),
        })
    return out

def list_rows(branch: str, category: str, date: str | None, limit: int):
    session = requests.Session()
    session.headers.update({"User-Agent": "sp-fetch/1.0"})
    collected, page = [], 1
    while len(collected) < limit:
        params = {"BranchName-equals": branch, "Category-equals": category,
                  "grid-sort": "Date", "grid-dir": "Desc", "grid-page": str(page)}
        if date: params["Date-equals"] = to_portal_date(date)
        html, final_url = fetch_page(session, params)
        rows = parse_table(html, final_url)
        if not rows: break
        collected.extend(rows)
        if len(rows) < 10: break
        page += 1
    return collected[:limit]

# ---------- download helpers ----------
def http_download(url: str, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists(): return path
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(131072):
                if chunk: f.write(chunk)
    return path

# ---------- XML parsing ----------
def detect_kind_from_name(filename: str) -> str:
    # PromoFull..., PriceFull..., etc.
    m = re.search(r'(PromoFull|Promo|PriceFull|Price)', filename, re.I)
    return m.group(1) if m else "unknown"

def iter_orderxml_lines(fobj: io.BufferedReader):
    # Generic <OrderXml><Envelope><Header><Details><Line>…</Line></Details></Header></Envelope>…
    # We stream <Line> elements, flatten child tags to a dict.
    context = ET.iterparse(fobj, events=("end",))
    for event, elem in context:
        if elem.tag.endswith("Line"):
            row = {child.tag.split('}',1)[-1]: (child.text or "").strip() for child in elem}
            yield row
            elem.clear()

def write_csv(rows, cols, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows: w.writerow(r)
    return out_path

def normalize_pricefull(line: dict) -> dict:
    # Keep raw keys if missing; all IDs as strings
    return {
        "PriceUpdateDate": line.get("PriceUpdateDate",""),
        "ItemCode": (line.get("ItemCode") or "").strip(),
        "ItemName": line.get("ItemName",""),
        "ManufacturerName": line.get("ManufacturerName","") or line.get("manufacturer",""),
        "ManufactureCountry": line.get("ManufactureCountry","") or line.get("country",""),
        "ManufacturerItemDescription": line.get("ManufacturerItemDescription","") or line.get("description",""),
        "UnitQty": line.get("UnitQty",""),
        "Quantity": line.get("Quantity",""),
        "UnitOfMeasure": line.get("UnitOfMeasure",""),
        "blsWeighted": line.get("blsWeighted",""),
        "QtyInPackage": line.get("QtyInPackage",""),
        "ItemPrice": line.get("ItemPrice",""),
        "UnitOfMeasurePrice": line.get("UnitOfMeasurePrice",""),
        "AllowDiscount": line.get("AllowDiscount",""),
        "ItemStatus": line.get("ItemStatus",""),
    }

def normalize_promofull(line: dict) -> dict:
    return {
        "PriceUpdateDate": line.get("PriceUpdateDate",""),
        "ItemCode": (line.get("ItemCode") or "").strip(),
        "IsGiftItem": line.get("IsGiftItem",""),
        "RewardType": line.get("RewardType",""),
        "AllowMultipleDiscounts": line.get("AllowMultipleDiscounts",""),
        "PromotionId": line.get("PromotionId",""),
    }

def parse_gz_to_csv(gz_path: Path, out_dir: Path) -> Path:
    """gunzip + parse, write normalized CSV based on kind."""
    kind = detect_kind_from_name(gz_path.name)
    xml_name = gz_path.stem  # drop .gz
    out_csv = out_dir / f"{xml_name}.csv"

    with gzip.open(gz_path, "rb") as gz:
        # Check root quickly
        head = gz.peek(2000)
        # Reopen because iterparse needs full stream position
    with gzip.open(gz_path, "rb") as gz:
        rows = iter_orderxml_lines(gz)
        if kind.lower().startswith("pricefull"):
            cols = ["PriceUpdateDate","ItemCode","ItemName","ManufacturerName","ManufactureCountry",
                    "ManufacturerItemDescription","UnitQty","Quantity","UnitOfMeasure","blsWeighted",
                    "QtyInPackage","ItemPrice","UnitOfMeasurePrice","AllowDiscount","ItemStatus"]
            norm = (normalize_pricefull(r) for r in rows)
            return write_csv(norm, cols, out_csv)
        elif kind.lower().startswith("promofull"):
            cols = ["PriceUpdateDate","ItemCode","IsGiftItem","RewardType","AllowMultipleDiscounts","PromotionId"]
            norm = (normalize_promofull(r) for r in rows)
            return write_csv(norm, cols, out_csv)
        else:
            # Fallback: dump all keys encountered (first 200 lines to infer)
            seen = set()
            buf = []
            for i, r in enumerate(rows):
                buf.append(r); seen |= set(r.keys())
                if i > 500: break
            cols = sorted(seen)
            # Re-parse full file now that we know cols
            with gzip.open(gz_path, "rb") as gz2:
                rows2 = iter_orderxml_lines(gz2)
                return write_csv(rows2, cols, out_csv)

def main():
    ap = argparse.ArgumentParser(description="Fetch & parse Super-Pharm files for a branch/date/category")
    ap.add_argument("--branch", required=True, help="e.g. 'סופר-פארם קרית אתא'")
    ap.add_argument("--category", required=True, choices=["PriceFull","PromoFull","Price","Promo"])
    ap.add_argument("--date", required=True, help="YYYY-MM-DD or DD/MM/YYYY")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", default="./sp_data")
    args = ap.parse_args()

    out = Path(args.out)
    raw_dir = out / "raw"
    csv_dir = out / "csv"

    rows = list_rows(args.branch, args.category, args.date, args.limit)
    if not rows:
        print("No portal rows found. Check branch/category/date.")
        sys.exit(2)

    print(f"[info] found {len(rows)} rows")
    for r in rows:
        if not r["href"]: continue
        gz_path = raw_dir / Path(r["href"]).name
        http_download(r["href"], gz_path)
        csv_path = parse_gz_to_csv(gz_path, csv_dir)
        print(f"[ok] {gz_path.name} -> {csv_path.name}")

if __name__ == "__main__":
    main()
