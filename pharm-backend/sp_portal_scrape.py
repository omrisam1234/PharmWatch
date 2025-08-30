#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, sys, urllib.parse
from datetime import datetime
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://prices.super-pharm.co.il/"

def to_portal_date(s: str | None) -> str | None:
    """Accept 'YYYY-MM-DD' or 'DD/MM/YYYY' and return 'DD/MM/YYYY' (portal expects this)."""
    if not s:
        return None
    s = s.strip()
    try:
        # ISO -> portal
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        pass
    # assume already dd/MM/yyyy (or the user passed correctly)
    return s

def fetch_page(session: requests.Session, params: dict) -> tuple[str, str]:
    r = session.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.text, r.url  # final_url helps resolve relative links

def parse_table(html: str, base_for_links: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.mvc-grid") or soup.find("table")
    if not table:
        return []

    # prefer tbody rows
    body = table.find("tbody") or table
    out = []
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        # find the download <a href="...gz">
        href = None
        for td in tds:
            a = td.find("a", href=True)
            if a and (a["href"].endswith(".gz") or "/Download/" in a["href"]):
                href = urllib.parse.urljoin(base_for_links, a["href"])
                break

        # columns typically: [הורדה, סניף, קטגוריה, תאריך, שם, מס]
        def txt(i):
            return tds[i].get_text(strip=True) if i < len(tds) else ""

        row = {
            "download_url": href,
            "branch": txt(1),
            "category": txt(2),
            "date": txt(3),
            "name": txt(4),
        }
        out.append(row)
    return out

def list_rows(branch_name: str, category: str, date_str: str | None, limit: int):
    portal_date = to_portal_date(date_str)
    session = requests.Session()
    session.headers.update({"User-Agent": "superpharm-scraper/1.0"})

    collected = []
    page = 1
    while len(collected) < limit:
        params = {
            "BranchName-equals": branch_name,
            "Category-equals": category,
            # sort newest first (grid options used by mvc-grid)
            "grid-sort": "Date",
            "grid-dir": "Desc",
            "grid-page": str(page),
        }
        if portal_date:
            params["Date-equals"] = portal_date

        html, final_url = fetch_page(session, params)
        rows = parse_table(html, final_url)
        if not rows:
            break
        collected.extend(rows)
        # stop if looks like last page (heuristic: page shorter than usual)
        if len(rows) < 10:
            break
        page += 1

    return collected[:limit]

def download(url: str, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    fn = url.split("/")[-1]
    path = os.path.join(out_dir, fn)
    if os.path.exists(path):
        print(f"exists: {path}")
        return path
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(131072):
                if chunk:
                    f.write(chunk)
    print(f"saved: {path}  ({os.path.getsize(path):,} bytes)")
    return path

def main():
    ap = argparse.ArgumentParser(description="List & download Super-Pharm files from prices.super-pharm.co.il")
    ap.add_argument("--branch", required=True, help="Exact branch name, e.g. 'סופר-פארם קרית אתא'")
    ap.add_argument("--category", required=True, choices=["Price", "PriceFull", "Promo", "PromoFull"])
    ap.add_argument("--date", default=None, help="YYYY-MM-DD or DD/MM/YYYY (portal expects DD/MM/YYYY)")
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--download", action="store_true", help="Also download the matched files")
    ap.add_argument("--out", default="./sp_data/raw")
    args = ap.parse_args()

    rows = list_rows(args.branch, args.category, args.date, args.limit)
    if not rows:
        print("No rows found. Check branch/category/date spelling.")
        sys.exit(2)

    for i, r in enumerate(rows, 1):
        print(f"{i:3d}. {r['date']:<12} {r['category']:<9} {r['branch']:<24} {r['name']}")
        if r["download_url"]:
            print(f"     → {r['download_url']}")

    if args.download:
        s = requests.Session()
        s.headers.update({"User-Agent": "superpharm-scraper/1.0"})
        for r in rows:
            if r["download_url"]:
                try:
                    download(r["download_url"], args.out)
                except Exception as e:
                    print("download failed:", r["download_url"], e)

if __name__ == "__main__":
    main()
