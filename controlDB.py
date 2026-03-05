"""
controlDB.py – Supabase CRUD utilities for stock meta information
+ pykrx data fetching helpers.

Environment variables required:
    SUPABASE_URL  – your project URL  (e.g. https://xxxx.supabase.co)
    SUPABASE_KEY  – service-role or anon key

Default table schema (create once in Supabase SQL editor):
    CREATE TABLE stock_meta (
        ticker      TEXT PRIMARY KEY,
        name        TEXT,
        market      TEXT,           -- KOSPI / KOSDAQ / KONEX
        sector      TEXT,
        industry    TEXT,
        listed_date DATE,
        extra       JSONB,
        updated_at  TIMESTAMPTZ DEFAULT now()
    );
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv
from pykrx import stock as krx
from supabase import Client, create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

def get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------

TABLE = "test"


def fetch_all(client: Client) -> list[dict]:
    """Return every row in stock_meta."""
    res = client.table(TABLE).select("*").execute()
    return res.data


def fetch_by_ticker(client: Client, ticker: str) -> dict | None:
    """Return a single row by ticker, or None if not found."""
    res = client.table(TABLE).select("*").eq("ticker", ticker).execute()
    return res.data[0] if res.data else None


def upsert_stock(client: Client, record: dict) -> dict:
    """Insert or update a stock_meta row.

    ``record`` must contain at least ``ticker``.
    ``updated_at`` is set automatically.
    """
    record = {**record, "updated_at": datetime.utcnow().isoformat()}
    res = client.table(TABLE).upsert(record).execute()
    return res.data[0] if res.data else {}


def update_stock(client: Client, ticker: str, fields: dict) -> dict:
    """Partially update fields for an existing ticker."""
    fields = {**fields, "updated_at": datetime.utcnow().isoformat()}
    res = client.table(TABLE).update(fields).eq("ticker", ticker).execute()
    return res.data[0] if res.data else {}


def delete_stock(client: Client, ticker: str) -> bool:
    """Delete a row by ticker. Returns True if a row was deleted."""
    res = client.table(TABLE).delete().eq("ticker", ticker).execute()
    return len(res.data) > 0


def bulk_upsert(client: Client, records: list[dict]) -> list[dict]:
    """Upsert multiple rows at once."""
    now = datetime.utcnow().isoformat()
    stamped = [{**r, "updated_at": now} for r in records]
    res = client.table(TABLE).upsert(stamped).execute()
    return res.data


# ---------------------------------------------------------------------------
# pykrx – market / ticker helpers
# ---------------------------------------------------------------------------

def get_tickers(market: str = "KOSPI", base_date: str | None = None) -> list[str]:
    """Return all tickers for a market on a given date (YYYYMMDD).

    Defaults to today if *base_date* is omitted.
    market: 'KOSPI' | 'KOSDAQ' | 'KONEX'
    """
    d = base_date or date.today().strftime("%Y%m%d")
    return krx.get_market_ticker_list(d, market=market)


def get_ticker_name(ticker: str) -> str:
    """Resolve a ticker code to its Korean company name."""
    return krx.get_market_ticker_name(ticker)


def get_ohlcv(
    ticker: str,
    start: str,
    end: str,
    market: str = "KOSPI",
) -> "pandas.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    """Daily OHLCV DataFrame for *ticker* between start/end (YYYYMMDD)."""
    return krx.get_market_ohlcv(start, end, ticker)


def get_fundamental(
    ticker: str,
    start: str,
    end: str,
) -> "pandas.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    """Daily fundamental data (BPS, PER, PBR, EPS, DIV, DPS)."""
    return krx.get_market_fundamental(start, end, ticker)


def get_market_cap(
    ticker: str,
    start: str,
    end: str,
) -> "pandas.DataFrame":  # type: ignore[name-defined]  # noqa: F821
    """Daily market cap / shares outstanding."""
    return krx.get_market_cap(start, end, ticker)


def build_stock_meta(ticker: str, market: str = "KOSPI") -> dict[str, Any]:
    """Build a stock_meta record dict from pykrx data for a single ticker."""
    name = get_ticker_name(ticker)
    today = date.today().strftime("%Y%m%d")

    try:
        fund = get_fundamental(ticker, today, today)
        extra: dict = fund.iloc[-1].to_dict() if not fund.empty else {}
    except Exception:
        extra = {}

    return {
        "ticker": ticker,
        "name": name,
        "market": market,
        "extra": extra,
    }


# ---------------------------------------------------------------------------
# Sync helpers  (fetch from pykrx → upsert to Supabase)
# ---------------------------------------------------------------------------

def sync_market(
    client: Client,
    market: str = "KOSPI",
    base_date: str | None = None,
) -> list[dict]:
    """Fetch all tickers for *market* and upsert their meta into Supabase."""
    tickers = get_tickers(market, base_date)
    records = []
    for t in tickers:
        try:
            records.append(build_stock_meta(t, market))
        except Exception as exc:
            print(f"[warn] {t}: {exc}")

    return bulk_upsert(client, records) if records else []


# ---------------------------------------------------------------------------
# Quick CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    db = get_client()

    # --- example: upsert a single record ---
    sample = {
        "ticker": "005930",
        "name": "삼성전자",
        "market": "KOSPI",
        "sector": "전기전자",
        "industry": "반도체",
    }
    print("upsert →", json.dumps(upsert_stock(db, sample), ensure_ascii=False, indent=2))

    # --- example: fetch ---
    row = fetch_by_ticker(db, "005930")
    print("fetch  →", json.dumps(row, ensure_ascii=False, indent=2))

    # --- example: update ---
    print("update →", json.dumps(update_stock(db, "005930", {"sector": "IT"}), ensure_ascii=False, indent=2))

    # --- example: delete ---
    print("delete →", delete_stock(db, "005930"))

    # --- pykrx quick check ---
    tickers = get_tickers("KOSPI")
    print(f"KOSPI tickers: {len(tickers)} found. Sample: {tickers[:5]}")
