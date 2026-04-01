"""
Phase 6: FRED, FMP & Yahoo Finance Indicator Ingestion Script

Fetches macro-economic (FRED) and financial/commodity time-series data
and populates the indicator_series and indicator_observation tables.

API Endpoints:
  - FRED (API):     https://api.stlouisfed.org/fred/series/observations (requires key)
  - FRED (Public):  https://fred.stlouisfed.org/graph/fredgraph.csv (no key required)
  - Yahoo Finance:  https://query1.finance.yahoo.com/v8/finance/chart/{symbol} (no key required)
  - FMP (optional): https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}

Environment Variables:
  - FRED_API_KEY:  API key for FRED (https://fred.stlouisfed.org/docs/api/api_key.html)
  - FMP_API_KEY:   API key for FMP (https://site.financialmodelingprep.com/) [optional]
  - DRY_RUN:       Set to 'true' for dry run mode (no DB writes)

Usage:
    # Without API keys (uses public endpoints)
    python ingest_indicators.py

    # With API keys
    export FRED_API_KEY="your_fred_key"
    export FMP_API_KEY="your_fmp_key"
    python ingest_indicators.py

    # Dry run (no DB writes)
    DRY_RUN=true python ingest_indicators.py
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
from surrealdb import AsyncSurreal

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from predictor_config import (
        SURREALDB_HOST,
        SURREALDB_USER,
        SURREALDB_PASS,
        SURREALDB_NS,
        SURREALDB_DB,
    )
except ImportError:
    # Fallback for local development
    SURREALDB_HOST = "http://127.0.0.1:8001"
    SURREALDB_USER = "root"
    SURREALDB_PASS = "root"
    SURREALDB_NS = "volva"
    SURREALDB_DB = "causal_graph"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("volva.ingest_indicators")

# =============================================================================
# API Configuration
# =============================================================================

FRED_API_KEY_ENV = "FRED_API_KEY"
FMP_API_KEY_ENV = "FMP_API_KEY"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3/historical-price-full"

# Public endpoints (no API key required)
FRED_PUBLIC_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
YAHOO_FINANCE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# Default date range for public endpoints
FRED_PUBLIC_START = "2020-01-01"
FRED_PUBLIC_END = "2026-03-27"
YAHOO_PERIOD1 = 1577836800  # 2020-01-01 Unix timestamp
YAHOO_PERIOD2 = 1711593600  # 2026-03-27 Unix timestamp

# Default lookback period (days)
DEFAULT_LOOKBACK_DAYS = 365

# =============================================================================
# Series Definitions (from strategy_phase6_fusion.md)
# =============================================================================

# FRED Series: Federal Reserve Economic Data
# Format: (series_id, name, description, frequency, unit)
FRED_SERIES = [
    # Central Bank & Monetary Policy
    (
        "FEDFUNDS",
        "Federal Funds Rate",
        "Effective Federal Funds Rate (monthly)",
        "monthly",
        "percent",
    ),
    (
        "DFF",
        "Federal Funds Rate (Daily)",
        "Effective Federal Funds Rate (daily)",
        "daily",
        "percent",
    ),
    (
        "T10Y3M",
        "10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity",
        "Yield curve indicator (daily)",
        "daily",
        "percent",
    ),
    (
        "T10Y2Y",
        "10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity",
        "Yield curve indicator (daily)",
        "daily",
        "percent",
    ),
    # Inflation & Pricing
    (
        "CPIAUCSL",
        "Consumer Price Index for All Urban Consumers: All Items",
        "Headline CPI (monthly)",
        "monthly",
        "index",
    ),
    (
        "CPILFESL",
        "Consumer Price Index for All Urban Consumers: All Items Less Food and Energy",
        "Core CPI (monthly)",
        "monthly",
        "index",
    ),
    (
        "PCE",
        "Personal Consumption Expenditures: Chain-type Price Index",
        "PCE Price Index (monthly)",
        "monthly",
        "index",
    ),
    (
        "PCETRIM12M1S",
        "Trimmed Mean PCE Inflation Rate",
        "Trimmed mean PCE inflation (monthly)",
        "monthly",
        "percent",
    ),
    # Employment & Labor Market
    (
        "UNRATE",
        "Unemployment Rate",
        "Civilian Unemployment Rate (monthly)",
        "monthly",
        "percent",
    ),
    (
        "PAYEMS",
        "All Employees: Total Nonfarm Payrolls",
        "Total Nonfarm Employment (monthly)",
        "monthly",
        "thousands",
    ),
    (
        "JTSJOL",
        "Job Openings: Total Nonfarm",
        "Job Openings (monthly)",
        "monthly",
        "thousands",
    ),
    # GDP & Monetary
    ("GDP", "Gross Domestic Product", "Real GDP (quarterly)", "quarterly", "billions"),
    (
        "WALCL",
        "Assets: Total Assets: Total Assets (Eliminated)",
        "Total Assets (weekly)",
        "weekly",
        "millions",
    ),
    # Housing & Real Estate
    (
        "HOUST",
        "Housing Starts: Total New Housing Units",
        "Housing Starts (monthly)",
        "monthly",
        "thousands",
    ),
    (
        "CSUSHPINSA",
        "Case-Shiller U.S. National Home Price Index",
        "S&P/Case-Shiller U.S. National Home Price Index (monthly)",
        "monthly",
        "index",
    ),
]

# FMP Symbols: Financial Modeling Prep
# Format: (symbol, name, description, frequency, unit)
FMP_SYMBOLS = [
    # Global Economy & Market Sentiment
    ("^GSPC", "S&P 500", "Standard & Poor's 500 Index (daily)", "daily", "index"),
    ("^IXIC", "NASDAQ Composite", "NASDAQ Composite Index (daily)", "daily", "index"),
    ("^DJI", "Dow Jones Industrial Average", "DJIA (daily)", "daily", "index"),
    # Energy & Resources
    (
        "CL=F",
        "Crude Oil WTI",
        "West Texas Intermediate Crude Oil Futures (daily)",
        "daily",
        "USD",
    ),
    ("BZ=F", "Crude Oil Brent", "Brent Crude Oil Futures (daily)", "daily", "USD"),
    ("NG=F", "Natural Gas", "Natural Gas Futures (daily)", "daily", "USD"),
    # Technology & Large Cap
    ("AAPL", "Apple Inc.", "Apple stock (daily)", "daily", "USD"),
    ("MSFT", "Microsoft Corporation", "Microsoft stock (daily)", "daily", "USD"),
    ("NVDA", "NVIDIA Corporation", "NVIDIA stock (daily)", "daily", "USD"),
    ("TSLA", "Tesla Inc.", "Tesla stock (daily)", "daily", "USD"),
    ("AMZN", "Amazon.com Inc.", "Amazon stock (daily)", "daily", "USD"),
    # Geopolitics & Safe Havens
    ("GC=F", "Gold", "Gold Futures (daily)", "daily", "USD"),
    ("SI=F", "Silver", "Silver Futures (daily)", "daily", "USD"),
]

# Yahoo Finance Symbols (public endpoint - no API key required)
# Format: (symbol, name, description, frequency, unit)
YAHOO_SYMBOLS = [
    # Test symbols per task requirements
    (
        "CL=F",
        "Crude Oil WTI",
        "West Texas Intermediate Crude Oil Futures (daily)",
        "daily",
        "USD",
    ),
    ("GC=F", "Gold", "Gold Futures (daily)", "daily", "USD"),
    ("AAPL", "Apple Inc.", "Apple stock (daily)", "daily", "USD"),
    ("SPY", "SPDR S&P 500 ETF", "S&P 500 ETF (daily)", "daily", "USD"),
    ("BTC-USD", "Bitcoin USD", "Bitcoin price in USD (daily)", "daily", "USD"),
]

# =============================================================================
# API Fetching Functions
# =============================================================================


async def fetch_fred_series(
    series_id: str,
    api_key: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> List[Dict[str, Any]]:
    """
    Fetch observations for a FRED series.

    Endpoint: GET https://api.stlouisfed.org/fred/series/observations
    Params:
        series_id: FRED series identifier (e.g., 'FEDFUNDS')
        api_key: FRED API key
        lookback_days: Number of days to look back (default 365)

    Returns:
        List of dicts with 'date' and 'value' keys
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.strftime("%Y-%m-%d"),
        "observation_end": end_date.strftime("%Y-%m-%d"),
    }

    logger.info(f"Fetching FRED series: {series_id}")

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(FRED_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            observations = []
            for obs in data.get("observations", []):
                if obs.get("value") and obs["value"] != ".":
                    observations.append(
                        {
                            "date": obs["date"],
                            "value": float(obs["value"]),
                        }
                    )

            logger.info(f"  Got {len(observations)} observations for {series_id}")
            return observations

    except httpx.HTTPStatusError as e:
        logger.error(f"  HTTP error fetching {series_id}: {e.response.status_code}")
        return []
    except Exception as e:
        logger.error(f"  Error fetching {series_id}: {e}")
        return []


async def fetch_fmp_series(
    symbol: str,
    api_key: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> List[Dict[str, Any]]:
    """
    Fetch historical price data for an FMP symbol.

    Endpoint: GET https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}
    Params:
        symbol: FMP symbol (e.g., '^GSPC', 'AAPL', 'CL=F')
        api_key: FMP API key
        lookback_days: Number of days to look back (default 365)

    Returns:
        List of dicts with 'date', 'value' (close), and 'change_percent' keys
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    params = {
        "apikey": api_key,
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
    }

    url = f"{FMP_BASE_URL}/{symbol}"
    logger.info(f"Fetching FMP symbol: {symbol}")

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            observations = []
            historical = data.get("historical", [])

            for obs in historical:
                date_str = obs.get("date")
                close = obs.get("close")
                change_pct = obs.get("changePercent")

                if date_str and close is not None:
                    try:
                        observations.append(
                            {
                                "date": date_str,
                                "value": float(close),
                                "change_percent": float(change_pct)
                                if change_pct
                                else None,
                            }
                        )
                    except (ValueError, TypeError):
                        continue

            logger.info(f"  Got {len(observations)} observations for {symbol}")
            return observations

    except httpx.HTTPStatusError as e:
        logger.error(f"  HTTP error fetching {symbol}: {e.response.status_code}")
        return []
    except Exception as e:
        logger.error(f"  Error fetching {symbol}: {e}")
        return []


async def fetch_fred_series_public(
    series_id: str,
    start_date: str = FRED_PUBLIC_START,
    end_date: str = FRED_PUBLIC_END,
) -> List[Dict[str, Any]]:
    """
    Fetch observations for a FRED series using the public CSV endpoint (no API key required).

    Endpoint: GET https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}
    Params:
        series_id: FRED series identifier (e.g., 'FEDFUNDS')
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of dicts with 'date' and 'value' keys
    """
    params = {
        "id": series_id,
        "cosd": start_date,
        "coed": end_date,
    }

    logger.info(f"Fetching FRED series (public CSV): {series_id}")
    logger.info(
        f"  Endpoint: {FRED_PUBLIC_CSV_URL}?id={series_id}&cosd={start_date}&coed={end_date}"
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(FRED_PUBLIC_CSV_URL, params=params)
            response.raise_for_status()

            # Parse CSV response
            # Format: date,value\n
            lines = response.text.strip().split("\n")
            observations = []

            # Skip header line
            for line in lines[1:]:
                parts = line.split(",")
                if len(parts) >= 2:
                    date_str = parts[0].strip()
                    value_str = parts[1].strip()

                    if value_str and value_str != ".":
                        try:
                            observations.append(
                                {
                                    "date": date_str,
                                    "value": float(value_str),
                                }
                            )
                        except ValueError:
                            continue

            logger.info(f"  Got {len(observations)} observations for {series_id}")
            return observations

    except httpx.HTTPStatusError as e:
        logger.error(f"  HTTP error fetching {series_id}: {e.response.status_code}")
        return []
    except Exception as e:
        logger.error(f"  Error fetching {series_id}: {e}")
        return []


async def fetch_yahoo_finance_series(
    symbol: str,
    period1: int = YAHOO_PERIOD1,
    period2: int = YAHOO_PERIOD2,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> List[Dict[str, Any]]:
    """
    Fetch historical price data from Yahoo Finance (no API key required).

    Endpoint: GET https://query1.finance.yahoo.com/v8/finance/chart/{symbol}
    Params:
        symbol: Yahoo Finance symbol (e.g., 'AAPL', 'CL=F', 'BTC-USD')
        period1: Start Unix timestamp
        period2: End Unix timestamp
        max_retries: Maximum number of retries on 429 (rate limit) errors
        retry_delay: Initial delay between retries in seconds

    Returns:
        List of dicts with 'date', 'value' (close), and 'change_percent' keys
    """
    url = f"{YAHOO_FINANCE_URL}/{symbol}"
    params = {
        "period1": period1,
        "period2": period2,
        "interval": "1d",
    }

    logger.info(f"Fetching Yahoo Finance symbol: {symbol}")
    logger.info(f"  Endpoint: {url}?period1={period1}&period2={period2}&interval=1d")

    last_error = None
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url, params=params)

                if response.status_code == 429:
                    # Rate limited - retry with backoff
                    last_error = (
                        f"429 Too Many Requests (attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        delay = retry_delay * (2**attempt)  # Exponential backoff
                        logger.warning(f"  Rate limited, retrying in {delay:.1f}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(
                            f"  HTTP error fetching {symbol}: 429 Rate Limited after {max_retries} attempts"
                        )
                        return []

                response.raise_for_status()
                data = response.json()

                chart_result = data.get("chart", {}).get("result", [])
                if not chart_result:
                    logger.warning(f"  No data returned for {symbol}")
                    return []

                result_data = chart_result[0]
                timestamps = result_data.get("timestamp", [])
                quote = result_data.get("indicators", {}).get("quote", [{}])[0]
                closes = quote.get("close", [])

                observations = []
                for i, ts in enumerate(timestamps):
                    if i < len(closes) and closes[i] is not None:
                        # Convert Unix timestamp to date string
                        date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        observations.append(
                            {
                                "date": date_str,
                                "value": float(closes[i]),
                                "change_percent": None,  # Yahoo doesn't provide this in basic chart endpoint
                            }
                        )

                logger.info(f"  Got {len(observations)} observations for {symbol}")
                return observations

        except httpx.HTTPStatusError as e:
            last_error = f"HTTP {e.response.status_code}"
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)
                logger.warning(
                    f"  HTTP error ({last_error}), retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                continue
            logger.error(f"  HTTP error fetching {symbol}: {e.response.status_code}")
            return []
        except Exception as e:
            last_error = str(e)
            logger.error(f"  Error fetching {symbol}: {e}")
            return []

    logger.error(f"  Failed after {max_retries} attempts: {last_error}")
    return []


# =============================================================================
# Database Operations
# =============================================================================


async def ensure_series_exists(
    db: AsyncSurreal,
    series_id: str,
    name: str,
    source: str,
    description: str,
    frequency: str,
    unit: str,
) -> Optional[str]:
    """
    Ensure a series exists in indicator_series table.
    Returns the record ID if successful, None if failed.
    """
    # Check if series already exists
    try:
        existing = await db.query(
            "SELECT id FROM indicator_series WHERE series_id = $series_id LIMIT 1;",
            {"series_id": series_id},
        )

        # Handle SurrealDB response format
        if isinstance(existing, list) and len(existing) > 0:
            if isinstance(existing[0], dict) and "id" in existing[0]:
                record_id = existing[0]["id"]
                logger.debug(f"  Series {series_id} already exists: {record_id}")
                return record_id
        elif isinstance(existing, dict) and "id" in existing:
            record_id = existing["id"]
            logger.debug(f"  Series {series_id} already exists: {record_id}")
            return record_id
    except Exception as e:
        logger.warning(f"  Error checking existing series {series_id}: {e}")

    # Insert new series
    try:
        result = await db.create(
            "indicator_series",
            {
                "name": name,
                "series_id": series_id,
                "source": source,
                "description": description,
                "frequency": frequency,
                "unit": unit,
                "metadata": {"created_at": datetime.now().isoformat()},
            },
        )
        # Handle SurrealDB response format (returns dict, not list)
        if isinstance(result, dict) and "id" in result:
            record_id = result["id"]
            logger.info(f"  Created series: {series_id} -> {record_id}")
            return record_id
        elif (
            isinstance(result, list)
            and len(result) > 0
            and isinstance(result[0], dict)
            and "id" in result[0]
        ):
            record_id = result[0]["id"]
            logger.info(f"  Created series: {series_id} -> {record_id}")
            return record_id
        else:
            logger.error(f"  Unexpected result format for {series_id}: {result}")
            return None
    except Exception as e:
        logger.error(f"  Error creating series {series_id}: {e}")
        return None


async def get_existing_observation_dates(
    db: AsyncSurreal,
    series_record_id: str,
) -> set:
    """
    Get set of existing observation dates for a series.
    Used for idempotency check.
    """
    try:
        result = await db.query(
            "SELECT date FROM indicator_observation WHERE series_id = $series_id;",
            {"series_id": series_record_id},
        )

        dates = set()
        # Handle SurrealDB response format
        records = []
        if isinstance(result, list):
            records = result
        elif isinstance(result, dict):
            # Single record returned
            records = [result]

        for record in records:
            if isinstance(record, dict):
                date_val = record.get("date")
                if date_val:
                    # Handle datetime object or string
                    if isinstance(date_val, datetime):
                        dates.add(date_val.strftime("%Y-%m-%d"))
                    else:
                        dates.add(str(date_val)[:10])

        return dates
    except Exception as e:
        logger.warning(f"  Error fetching existing dates: {e}")
        return set()


async def insert_observation_idempotent(
    db: AsyncSurreal,
    series_record_id: str,
    date_str: str,
    value: float,
    change_percent: Optional[float] = None,
    dry_run: bool = False,
) -> bool:
    """
    Insert observation only if it doesn't already exist.
    Uses hash-based idempotency: hash(series_record_id + date_str).
    Returns True if inserted (or already exists), False on error.
    """
    # Parse date for comparison
    try:
        obs_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logger.warning(f"  Invalid date format: {date_str}")
        return False

    # Compute idempotency hash
    hash_input = f"{series_record_id}:{date_str}"
    dedup_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:32]

    # Check if already exists by hash
    try:
        existing = await db.query(
            "SELECT id FROM indicator_observation WHERE metadata.dedup_hash = $dedup_hash LIMIT 1;",
            {"dedup_hash": dedup_hash},
        )

        # Handle SurrealDB response format
        if isinstance(existing, list) and len(existing) > 0:
            if isinstance(existing[0], dict) and "id" in existing[0]:
                logger.debug(
                    f"  Observation exists (hash): {series_record_id} @ {date_str}"
                )
                return True
        elif isinstance(existing, dict) and "id" in existing:
            logger.debug(
                f"  Observation exists (hash): {series_record_id} @ {date_str}"
            )
            return True
    except Exception as e:
        logger.warning(f"  Error checking existing observation: {e}")

    if dry_run:
        logger.info(
            f"  [DRY RUN] Would insert: {series_record_id} @ {date_str} = {value}"
        )
        return True

    # Insert new observation with hash-based idempotency
    try:
        await db.create(
            "indicator_observation",
            {
                "series_id": series_record_id,
                "date": obs_date,
                "value": value,
                "change_percent": change_percent,
                "metadata": {
                    "ingested_at": datetime.now().isoformat(),
                    "dedup_hash": dedup_hash,
                },
            },
        )
        return True
    except Exception as e:
        logger.error(f"  Error inserting observation: {e}")
        return False


# =============================================================================
# Main Ingestion Pipeline
# =============================================================================


async def ingest_fred_indicators(
    db: AsyncSurreal,
    api_key: str,
    dry_run: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, int]:
    """
    Ingest all FRED series.
    Returns dict with 'series_count' and 'observation_count'.
    """
    results = {"series_count": 0, "observation_count": 0}

    for series_id, name, description, frequency, unit in FRED_SERIES:
        # Ensure series exists
        record_id = await ensure_series_exists(
            db, f"FRED:{series_id}", name, "FRED", description, frequency, unit
        )

        if not record_id:
            logger.error(f"  Failed to get/create series: {series_id}")
            continue

        results["series_count"] += 1

        # Fetch data from FRED
        observations = await fetch_fred_series(series_id, api_key, lookback_days)

        if not observations:
            logger.warning(f"  No observations fetched for {series_id}")
            continue

        # Get existing dates for idempotency
        existing_dates = await get_existing_observation_dates(db, record_id)

        # Insert observations (idempotent)
        for obs in observations:
            date_str = obs["date"]
            if date_str[:10] in existing_dates:
                logger.debug(f"  Skipping existing: {series_id} @ {date_str}")
                continue

            success = await insert_observation_idempotent(
                db, record_id, date_str, obs["value"], None, dry_run
            )
            if success:
                results["observation_count"] += 1

    return results


async def ingest_fmp_indicators(
    db: AsyncSurreal,
    api_key: str,
    dry_run: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, int]:
    """
    Ingest all FMP symbols.
    Returns dict with 'series_count' and 'observation_count'.
    """
    results = {"series_count": 0, "observation_count": 0}

    for symbol, name, description, frequency, unit in FMP_SYMBOLS:
        # Ensure series exists
        record_id = await ensure_series_exists(
            db, f"FMP:{symbol}", name, "FMP", description, frequency, unit
        )

        if not record_id:
            logger.error(f"  Failed to get/create series: {symbol}")
            continue

        results["series_count"] += 1

        # Fetch data from FMP
        observations = await fetch_fmp_series(symbol, api_key, lookback_days)

        if not observations:
            logger.warning(f"  No observations fetched for {symbol}")
            continue

        # Get existing dates for idempotency
        existing_dates = await get_existing_observation_dates(db, record_id)

        # Insert observations (idempotent)
        for obs in observations:
            date_str = obs["date"]
            if date_str[:10] in existing_dates:
                logger.debug(f"  Skipping existing: {symbol} @ {date_str}")
                continue

            success = await insert_observation_idempotent(
                db,
                record_id,
                date_str,
                obs["value"],
                obs.get("change_percent"),
                dry_run,
            )
            if success:
                results["observation_count"] += 1

    return results


async def ingest_fred_indicators_public(
    db: AsyncSurreal,
    series_filter: List[str] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Ingest FRED series using public CSV endpoint (no API key required).

    Args:
        db: SurrealDB connection
        series_filter: Optional list of specific series IDs to ingest (e.g., ['fedfunds', 'unrate'])
        dry_run: If True, don't write to database

    Returns:
        dict with 'series_count' and 'observation_count'
    """
    results = {"series_count": 0, "observation_count": 0}

    # Filter series if specified
    if series_filter:
        filtered_series = [
            (sid, name, desc, freq, unit)
            for sid, name, desc, freq, unit in FRED_SERIES
            if sid.lower() in [s.lower() for s in series_filter]
        ]
    else:
        filtered_series = FRED_SERIES

    for series_id, name, description, frequency, unit in filtered_series:
        # Ensure series exists
        record_id = await ensure_series_exists(
            db, f"FRED:{series_id}", name, "FRED", description, frequency, unit
        )

        if not record_id:
            logger.error(f"  Failed to get/create series: {series_id}")
            continue

        results["series_count"] += 1

        # Fetch data from public FRED endpoint
        observations = await fetch_fred_series_public(series_id)

        if not observations:
            logger.warning(f"  No observations fetched for {series_id}")
            continue

        # Get existing dates for idempotency
        existing_dates = await get_existing_observation_dates(db, record_id)

        # Insert observations (idempotent)
        for obs in observations:
            date_str = obs["date"]
            if date_str[:10] in existing_dates:
                logger.debug(f"  Skipping existing: {series_id} @ {date_str}")
                continue

            success = await insert_observation_idempotent(
                db, record_id, date_str, obs["value"], None, dry_run
            )
            if success:
                results["observation_count"] += 1

    return results


async def ingest_yahoo_indicators(
    db: AsyncSurreal,
    symbols_filter: List[str] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Ingest symbols from Yahoo Finance (no API key required).

    Args:
        db: SurrealDB connection
        symbols_filter: Optional list of specific symbols to ingest (e.g., ['cl=f', 'gc=f'])
        dry_run: If True, don't write to database

    Returns:
        dict with 'series_count' and 'observation_count'
    """
    results = {"series_count": 0, "observation_count": 0}

    # Filter symbols if specified
    if symbols_filter:
        filtered_symbols = [
            (sym, name, desc, freq, unit)
            for sym, name, desc, freq, unit in YAHOO_SYMBOLS
            if sym.lower() in [s.lower() for s in symbols_filter]
        ]
    else:
        filtered_symbols = YAHOO_SYMBOLS

    for symbol, name, description, frequency, unit in filtered_symbols:
        # Ensure series exists
        record_id = await ensure_series_exists(
            db, f"YAHOO:{symbol}", name, "YAHOO", description, frequency, unit
        )

        if not record_id:
            logger.error(f"  Failed to get/create series: {symbol}")
            continue

        results["series_count"] += 1

        # Fetch data from Yahoo Finance
        observations = await fetch_yahoo_finance_series(symbol)

        if not observations:
            logger.warning(f"  No observations fetched for {symbol}")
            continue

        # Get existing dates for idempotency
        existing_dates = await get_existing_observation_dates(db, record_id)

        # Insert observations (idempotent)
        for obs in observations:
            date_str = obs["date"]
            if date_str[:10] in existing_dates:
                logger.debug(f"  Skipping existing: {symbol} @ {date_str}")
                continue

            success = await insert_observation_idempotent(
                db,
                record_id,
                date_str,
                obs["value"],
                obs.get("change_percent"),
                dry_run,
            )
            if success:
                results["observation_count"] += 1

    return results


async def run_ingestion(
    dry_run: bool = False,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    fred_series_filter: List[str] = None,
    yahoo_symbols_filter: List[str] = None,
) -> Dict[str, Any]:
    """
    Main entry point for indicator ingestion.

    Args:
        dry_run: If True, don't write to database
        lookback_days: Number of days to look back (for API-based sources)
        fred_series_filter: Optional list of FRED series IDs to ingest
        yahoo_symbols_filter: Optional list of Yahoo Finance symbols to ingest
    """
    fred_api_key = os.environ.get(FRED_API_KEY_ENV)
    fmp_api_key = os.environ.get(FMP_API_KEY_ENV)

    logger.info(
        f"Starting indicator ingestion (dry_run={dry_run}, lookback={lookback_days} days)"
    )
    logger.info("=" * 60)
    logger.info("Endpoints configuration:")
    if fred_api_key:
        logger.info(f"  FRED API: {FRED_BASE_URL} (API key available)")
    else:
        logger.info(f"  FRED Public CSV: {FRED_PUBLIC_CSV_URL} (no API key)")
    if fmp_api_key:
        logger.info(f"  FMP API: {FMP_BASE_URL} (API key available)")
    logger.info(f"  Yahoo Finance: {YAHOO_FINANCE_URL} (no API key required)")
    logger.info("=" * 60)

    results = {
        "fred": {"status": "skipped", "series_count": 0, "observation_count": 0},
        "fred_public": {"status": "skipped", "series_count": 0, "observation_count": 0},
        "fmp": {"status": "skipped", "series_count": 0, "observation_count": 0},
        "yahoo": {"status": "skipped", "series_count": 0, "observation_count": 0},
        "endpoints_used": [],
    }

    try:
        async with AsyncSurreal(SURREALDB_HOST) as db:
            await db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
            await db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)

            # Verify tables exist
            try:
                await db.query("SELECT * FROM indicator_series LIMIT 1;")
                await db.query("SELECT * FROM indicator_observation LIMIT 1;")
            except Exception as e:
                logger.error(f"Table verification failed: {e}")
                return {"status": "error", "message": f"Table verification failed: {e}"}

            # Ingest FRED data (API-based)
            if fred_api_key:
                logger.info("=" * 60)
                logger.info("Ingesting FRED indicators (API)...")
                try:
                    fred_results = await ingest_fred_indicators(
                        db, fred_api_key, dry_run, lookback_days
                    )
                    results["fred"] = {
                        "status": "success",
                        **fred_results,
                    }
                    results["endpoints_used"].append(
                        {
                            "source": "fred",
                            "endpoint": FRED_BASE_URL,
                            "type": "api",
                        }
                    )
                except Exception as e:
                    logger.error(f"FRED ingestion failed: {e}")
                    results["fred"] = {"status": "error", "message": str(e)}
            else:
                # Use public CSV endpoint for FRED
                logger.info("=" * 60)
                logger.info("Ingesting FRED indicators (public CSV)...")
                try:
                    fred_results = await ingest_fred_indicators_public(
                        db, fred_series_filter, dry_run
                    )
                    results["fred_public"] = {
                        "status": "success",
                        **fred_results,
                    }
                    results["endpoints_used"].append(
                        {
                            "source": "fred_public",
                            "endpoint": f"{FRED_PUBLIC_CSV_URL}?id={{series_id}}&cosd={FRED_PUBLIC_START}&coed={FRED_PUBLIC_END}",
                            "type": "public_csv",
                        }
                    )
                except Exception as e:
                    logger.error(f"FRED public ingestion failed: {e}")
                    results["fred_public"] = {"status": "error", "message": str(e)}

            # Ingest Yahoo Finance data (public endpoint)
            logger.info("=" * 60)
            logger.info("Ingesting Yahoo Finance indicators...")
            try:
                yahoo_results = await ingest_yahoo_indicators(
                    db, yahoo_symbols_filter, dry_run
                )
                results["yahoo"] = {
                    "status": "success",
                    **yahoo_results,
                }
                results["endpoints_used"].append(
                    {
                        "source": "yahoo",
                        "endpoint": f"{YAHOO_FINANCE_URL}/{{symbol}}?period1={YAHOO_PERIOD1}&period2={YAHOO_PERIOD2}&interval=1d",
                        "type": "public_json",
                    }
                )
            except Exception as e:
                logger.error(f"Yahoo ingestion failed: {e}")
                results["yahoo"] = {"status": "error", "message": str(e)}

            # Ingest FMP data (API-based, only if key available)
            if fmp_api_key:
                logger.info("=" * 60)
                logger.info("Ingesting FMP indicators...")
                try:
                    fmp_results = await ingest_fmp_indicators(
                        db, fmp_api_key, dry_run, lookback_days
                    )
                    results["fmp"] = {
                        "status": "success",
                        **fmp_results,
                    }
                    results["endpoints_used"].append(
                        {
                            "source": "fmp",
                            "endpoint": FMP_BASE_URL,
                            "type": "api",
                        }
                    )
                except Exception as e:
                    logger.error(f"FMP ingestion failed: {e}")
                    results["fmp"] = {"status": "error", "message": str(e)}

            logger.info("=" * 60)
            logger.info("Ingestion complete!")
            logger.info(f"Results: {json.dumps(results, indent=2)}")

            return {"status": "success", "results": results}

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return {"status": "error", "message": f"Database error: {e}"}


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """CLI entry point."""
    dry_run = os.environ.get("DRY_RUN", "").lower() == "true"
    lookback = int(os.environ.get("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))

    # Optional filters for specific series/symbols (comma-separated)
    fred_filter_str = os.environ.get("FRED_FILTER", "")
    yahoo_filter_str = os.environ.get("YAHOO_FILTER", "")

    fred_series_filter = (
        [s.strip() for s in fred_filter_str.split(",") if s.strip()]
        if fred_filter_str
        else None
    )
    yahoo_symbols_filter = (
        [s.strip() for s in yahoo_filter_str.split(",") if s.strip()]
        if yahoo_filter_str
        else None
    )

    if dry_run:
        logger.info("DRY RUN MODE - No data will be written to database")

    if fred_series_filter:
        logger.info(f"FRED filter active: {fred_series_filter}")
    if yahoo_symbols_filter:
        logger.info(f"Yahoo filter active: {yahoo_symbols_filter}")

    result = asyncio.run(
        run_ingestion(
            dry_run=dry_run,
            lookback_days=lookback,
            fred_series_filter=fred_series_filter,
            yahoo_symbols_filter=yahoo_symbols_filter,
        )
    )

    if result.get("status") == "success":
        print("\n✓ Ingestion completed successfully")
    else:
        print(f"\n✗ Ingestion failed: {result.get('message')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
