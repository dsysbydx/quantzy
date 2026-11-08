"""Build the insider-trade table from the SEC's Form 3/4/5 bulk data sets (2006 Q1 on).

    uv run python research/insider_data.py trades     # parse .cache/insider/raw/*.zip
    uv run python research/insider_data.py sic        # fetch each issuer's SIC code

Raw files: https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets,
one zip per quarter, downloaded to ``.cache/insider/raw``. SIC codes come from
``data.sec.gov/submissions/CIK##########.json``. The SEC requires a User-Agent with a contact
email; set ``SEC_USER_AGENT`` in the environment.

Kept: Form 4 (not amendments), non-derivative open-market purchases (code P, acquired) and
sales (code S, disposed), price and shares > 0, transaction no more than 30 days before the
filing. One row per transaction with the filing date (when it became public), the
reporting owner's roles, and the issuer.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(".cache/insider")
RAW = ROOT / "raw"
TRADES = ROOT / "trades.parquet"
SIC = ROOT / "sic.parquet"


def _read(z: zipfile.ZipFile, name: str, cols: list[str]) -> pd.DataFrame:
    with z.open(name) as f:
        return pd.read_csv(io.TextIOWrapper(f, "utf-8", errors="replace"), sep="\t",
                           usecols=cols, dtype=str, quoting=3, on_bad_lines="skip")


def parse_one(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        sub = _read(z, "SUBMISSION.tsv", ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE",
                                          "ISSUERCIK", "ISSUERTRADINGSYMBOL"])
        tr = _read(z, "NONDERIV_TRANS.tsv", ["ACCESSION_NUMBER", "TRANS_DATE", "TRANS_CODE",
                                             "TRANS_SHARES", "TRANS_PRICEPERSHARE",
                                             "TRANS_ACQUIRED_DISP_CD"])
        own = _read(z, "REPORTINGOWNER.tsv", ["ACCESSION_NUMBER", "RPTOWNERCIK",
                                              "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"])
    sub = sub[sub["DOCUMENT_TYPE"] == "4"]
    tr = tr[tr["TRANS_CODE"].isin(["P", "S"])]
    # one owner per filing (the first); joint filings repeat the same trade
    own = own.drop_duplicates("ACCESSION_NUMBER")
    df = tr.merge(sub, on="ACCESSION_NUMBER").merge(own, on="ACCESSION_NUMBER", how="left")
    df["shares"] = pd.to_numeric(df["TRANS_SHARES"], errors="coerce")
    df["price"] = pd.to_numeric(df["TRANS_PRICEPERSHARE"], errors="coerce")
    df["filed"] = pd.to_datetime(df["FILING_DATE"], format="%d-%b-%Y", errors="coerce")
    df["traded"] = pd.to_datetime(df["TRANS_DATE"], format="%d-%b-%Y", errors="coerce")
    df = df[(df["shares"] > 0) & (df["price"] > 0) & df["filed"].notna() & df["traded"].notna()]
    df = df[((df["TRANS_CODE"] == "P") & (df["TRANS_ACQUIRED_DISP_CD"] == "A"))
            | ((df["TRANS_CODE"] == "S") & (df["TRANS_ACQUIRED_DISP_CD"] == "D"))]
    lag = (df["filed"] - df["traded"]).dt.days
    df = df[(lag >= 0) & (lag <= 30)]
    rel = df["RPTOWNER_RELATIONSHIP"].fillna("")
    out = pd.DataFrame({
        "accession": df["ACCESSION_NUMBER"], "filed": df["filed"], "traded": df["traded"],
        "cik": df["ISSUERCIK"].str.lstrip("0"), "symbol": df["ISSUERTRADINGSYMBOL"],
        "owner": df["RPTOWNERCIK"].str.lstrip("0"), "buy": df["TRANS_CODE"] == "P",
        "value": df["shares"] * df["price"],
        "director": rel.str.contains("Director"), "officer": rel.str.contains("Officer"),
        "ten_pct": rel.str.contains("TenPercentOwner"),
        "title": df["RPTOWNER_TITLE"].fillna("").str.upper(),
    })
    return out


def build_trades() -> pd.DataFrame:
    parts = []
    for p in sorted(RAW.glob("*_form345.zip")):
        d = parse_one(p)
        parts.append(d)
        print(f"{p.name}: {len(d):,} trades, {d['buy'].mean():.1%} buys", flush=True)
    df = pd.concat(parts, ignore_index=True)
    # the bulk sets can repeat a filing across quarters; keep one copy of each trade
    df = df.drop_duplicates(["accession", "owner", "traded", "buy", "value"])
    df.to_parquet(TRADES)
    print(f"wrote {TRADES}: {len(df):,} trades, {df['cik'].nunique():,} issuers, "
          f"{df['filed'].min().date()} .. {df['filed'].max().date()}")
    return df


def fetch_sic() -> pd.DataFrame:
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        sys.exit("set SEC_USER_AGENT to 'name email' (the SEC requires a contact)")
    ciks = sorted(pd.read_parquet(TRADES, columns=["cik"])["cik"].unique(), key=int)
    have = pd.read_parquet(SIC) if SIC.exists() else pd.DataFrame(columns=["cik", "sic", "name"])
    todo = [c for c in ciks if c not in set(have["cik"])]
    print(f"{len(ciks):,} issuers, {len(todo):,} to fetch", flush=True)
    rows = []
    for i, c in enumerate(todo):
        url = f"https://data.sec.gov/submissions/CIK{int(c):010d}.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": ua})
            with urllib.request.urlopen(req, timeout=30) as r:
                j = json.load(r)
            rows.append({"cik": c, "sic": j.get("sic") or "", "name": j.get("name") or ""})
        except Exception as e:  # noqa: BLE001 -- record and move on
            rows.append({"cik": c, "sic": "", "name": f"ERROR {e}"})
        time.sleep(0.12)  # under the SEC's 10 requests a second
        if (i + 1) % 500 == 0 or i + 1 == len(todo):
            have = pd.concat([have, pd.DataFrame(rows)], ignore_index=True)
            have.to_parquet(SIC)
            rows = []
            print(f"  {i + 1:,} / {len(todo):,}", flush=True)
    return have


def sic_from_bulk() -> pd.DataFrame:
    """SIC codes from EDGAR's bulk ``submissions.zip`` (one JSON per CIK), downloaded to
    ``.cache/insider/submissions.zip``: much faster than one request per issuer."""
    ciks = set(pd.read_parquet(TRADES, columns=["cik"])["cik"].unique())
    rows = []
    with zipfile.ZipFile(ROOT / "submissions.zip") as z:
        names = {n[3:13].lstrip("0"): n for n in z.namelist()
                 if n.startswith("CIK") and n.endswith(".json") and "-submissions-" not in n}
        for c in ciks:
            n = names.get(c)
            if n is None:
                rows.append({"cik": c, "sic": "", "name": ""})
                continue
            j = json.loads(z.read(n))
            rows.append({"cik": c, "sic": j.get("sic") or "", "name": j.get("name") or ""})
    df = pd.DataFrame(rows)
    df.to_parquet(SIC)
    print(f"wrote {SIC}: {len(df):,} issuers, {(df['sic'] != '').mean():.1%} with a SIC")
    return df


if __name__ == "__main__":
    {"trades": build_trades, "sic": fetch_sic, "sic_bulk": sic_from_bulk}[sys.argv[1]]()
