#!/usr/bin/env python3
"""
Kalshi Scraper for Volva Cycle6 Alt Markets
Fetches prediction markets from Kalshi API and ingests as events into SurrealDB.

API Endpoint: https://api.elections.kalshi.com/ (requires auth for trading)
Note: Kalshi is CFTC-regulated; API requires authentication.
Public market data endpoint: GET /markets (status filter supported)
Dedup: market ticker (unique per Kalshi)
Category: kalshi
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

import httpx
from surrealdb import AsyncSurreal

# Add parent directory (news/events) to path for config import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    SURREALDB_HOST,
    SURREALDB_USER,
    SURREALDB_PASS,
    SURREALDB_NS,
    SURREALDB_DB,
)

# Kalshi API Configuration
KALSHI_API_BASE = "https://api.elections.kalshi.com"
KALSHI_API_MARKETS = f"{KALSHI_API_BASE}/markets"
MAX_MARKETS_PER_RUN = 100  # Limit per ingestion run

# Market Quality Filter Configuration
CLOSE_WINDOW_DAYS = 7  # Only ingest markets expiring within 7 days
MIN_VOLUME = 1000  # Minimum volume for high-vol filter
MIN_TURNOVER = 100  # Minimum turnover for liquidity

# Trivial market exclusion: tags that indicate non-causal markets
# Note: Kalshi has more serious markets, but still filter sports/entertainment
TRIVIAL_TAGS = {
    "sports", "entertainment", "gambling",
    "football", "basketball", "baseball", "soccer", "tennis", "golf",
    "olympics", "nba", "nfl", "mlb", "epl",
    "celebrity", "music", "movie", "tv_show",
}

# Trivial question patterns (case-insensitive matching)
# Kalshi markets tend to be more serious, but filter personal questions
TRIVIAL_PATTERNS = [
    r"\bwill i\b", r"\bmy\b", r"\bi will\b", r"\bi'm going to\b",
    r"\bi wake up\b", r"\bi go to bed\b", r"\bi eat\b", r"\bi drink\b",
    r"\bwhat time will i\b", r"\bwhen will i\b", r"\bhow long will i\b",
    r"\bdaily question\b", r"\bpoll of\b", r"\bi'm gonna\b",
    r"\bi’m\b", r"\bgoing to bed\b", r"\bwake up\b",
]

# Source info for Kalshi
KALSHI_SOURCE = {
    "name": "Kalshi",
    "url": KALSHI_API_MARKETS,
    "type": "api",
    "reliability": 0.8,  # CFTC-regulated, higher reliability
    "categories": ["kalshi", "prediction-market"],
}


class KalshiScraper:
    """Scraper for Kalshi prediction market data."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.stats = {
            "markets_fetched": 0,
            "events_created": 0,
            "duplicates_skipped": 0,
            "errors": [],
        }
        self.known_tickers: set = set()
        self.dedup_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "kalshi_dedup.json"
        )

    async def connect(self):
        """Connect to SurrealDB."""
        self.db = AsyncSurreal(SURREALDB_HOST)
        await self.db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
        await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)

    async def close(self):
        """Close SurrealDB connection."""
        if self.db:
            try:
                await self.db.close()
            except NotImplementedError:
                pass

    def load_dedup_tracker(self):
        """Load known market tickers from persistent storage."""
        if os.path.exists(self.dedup_file):
            try:
                with open(self.dedup_file, "r") as f:
                    data = json.load(f)
                    self.known_tickers = set(data.get("tickers", []))
                    print(f"Loaded {len(self.known_tickers)} known market tickers")
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load dedup tracker: {e}")
                self.known_tickers = set()

    def save_dedup_tracker(self):
        """Persist known market tickers to storage."""
        try:
            with open(self.dedup_file, "w") as f:
                json.dump(
                    {
                        "tickers": list(self.known_tickers),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
        except IOError as e:
            print(f"Warning: Could not save dedup tracker: {e}")

    async def upsert_source(self) -> str:
        """Register or update Kalshi source in source table."""
        url = KALSHI_SOURCE["url"]
        
        # Check if source exists
        result = await self.db.query(
            f"SELECT id FROM source WHERE url = '{url}' LIMIT 1;"
        )
        
        if result and len(result) > 0 and 'id' in result[0]:
            return str(result[0]['id'])

        # Create new source
        result = await self.db.create("source", {
            "url": url,
            "type": KALSHI_SOURCE["type"],
            "reliability": KALSHI_SOURCE["reliability"],
        })
        return str(result['id'])

    async def check_event_exists_by_ticker(self, ticker: str) -> bool:
        """Check if event with this ticker already exists."""
        if ticker in self.known_tickers:
            return True
        
        # Also check database
        result = await self.db.query(
            f"SELECT id FROM event WHERE metadata.kalshi_ticker = '{ticker}' LIMIT 1;"
        )
        if result and len(result) > 0 and 'id' in result[0]:
            self.known_tickers.add(ticker)
            return True
        return False

    def is_trivial_market(self, market: dict) -> tuple[bool, str]:
        """
        Check if a market is trivial (personal, sports, entertainment, etc.).
        
        Returns:
            (is_trivial, reason) - tuple of boolean and reason string
        """
        question = market.get("question", "").lower()
        tags = market.get("tags", [])
        sub_market = market.get("sub_market", "")
        question_lower = question.lower()
        
        # Check sub_market for trivial categories
        if sub_market:
            sub_lower = sub_market.lower() if isinstance(sub_market, str) else str(sub_market).lower()
            for trivial_tag in TRIVIAL_TAGS:
                if trivial_tag.lower() in sub_lower:
                    return True, f"trivial_submarket:{sub_lower}"
        
        # Check tags for trivial content
        for tag in tags:
            tag_lower = tag.lower() if isinstance(tag, str) else str(tag).lower()
            for trivial_tag in TRIVIAL_TAGS:
                if trivial_tag.lower() in tag_lower:
                    return True, f"trivial_tag:{tag_lower}"
        
        # Check question patterns for trivial content
        for pattern in TRIVIAL_PATTERNS:
            if re.search(pattern, question_lower, re.IGNORECASE):
                return True, f"trivial_pattern:{pattern}"
        
        return False, ""

    def should_ingest_market(self, market: dict) -> tuple[bool, str]:
        """
        Determine if a market should be ingested based on quality filters.
        
        Filters:
        1. Open status: market is open
        2. Near-close: expiration within CLOSE_WINDOW_DAYS days
        3. High-vol: vol >= MIN_VOLUME
        4. Top liquidity: turnover >= MIN_TURNOVER
        5. Non-trivial: not in trivial categories
        
        Returns:
            (should_ingest, reason) - tuple of boolean and reason string
        """
        ticker = market.get("ticker", "unknown")
        
        # Filter 1: Check status - only open markets
        status = market.get("status", "")
        if status != "open":
            return False, f"not_open:status={status}"
        
        # Filter 2: Near-close filter - must expire within 7 days
        expiration = market.get("expiration")
        if expiration:
            try:
                if isinstance(expiration, (int, float)):
                    exp_dt = datetime.fromtimestamp(expiration / 1000, tz=timezone.utc)
                elif isinstance(expiration, str):
                    exp_str = expiration.replace('Z', '+00:00')
                    exp_dt = datetime.fromisoformat(exp_str)
                else:
                    exp_dt = None
                
                if exp_dt:
                    now = datetime.now(timezone.utc)
                    days_until_exp = (exp_dt - now).total_seconds() / 86400
                    if days_until_exp > CLOSE_WINDOW_DAYS:
                        return False, f"too_far_out:{days_until_exp:.1f}d"
                    if days_until_exp < 0:
                        return False, "already_expired"
            except (ValueError, TypeError) as e:
                return False, f"timestamp_error:{e}"
        else:
            # No expiration - skip
            return False, "no_expiration"
        
        # Filter 3: High-vol filter - check vol
        volume = market.get("vol", 0) or 0
        if volume < MIN_VOLUME:
            return False, f"low_volume:{volume}"
        
        # Filter 4: Top liquidity filter - check turnover
        turnover = market.get("turnover", 0) or 0
        if turnover < MIN_TURNOVER:
            return False, f"low_turnover:{turnover}"
        
        # Filter 5: Non-trivial filter
        is_trivial, trivial_reason = self.is_trivial_market(market)
        if is_trivial:
            return False, trivial_reason
        
        return True, ""

    def parse_market_timestamp(self, market: dict) -> datetime:
        """Extract timestamp from market data."""
        # Priority: expiration > market_started > close_time
        ts = market.get("expiration") or market.get("market_started") or market.get("close_time")
        if ts:
            try:
                # Handle milliseconds timestamp
                if isinstance(ts, (int, float)):
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                # Handle ISO format
                if isinstance(ts, str):
                    if ts.endswith('Z'):
                        ts = ts[:-1] + '+00:00'
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except (ValueError, TypeError) as e:
                print(f"Timestamp parse error: {e}")
                pass
        return datetime.now(timezone.utc)

    def build_event_content(self, market: dict) -> str:
        """Build event content string from market data."""
        question = market.get("question", "")
        description = market.get("description", "")
        
        # Get current price/bid if available
        best_bid = market.get("best_bid", "")
        best_ask = market.get("best_ask", "")
        
        # Clean description (remove newlines)
        if description:
            description = description.replace("\n", " ").strip()
        
        content = question
        if description:
            content += f" | {description}"
        if best_bid or best_ask:
            content += f" | Bid: {best_bid} Ask: {best_ask}"
        
        return content[:2000]  # Truncate if too long

    async def create_event(self, market: dict, source_id: str) -> bool:
        """Create an event record in SurrealDB from market data."""
        ticker = market.get("ticker", "")
        question = market.get("question", "")
        
        # Parse timestamp
        timestamp = self.parse_market_timestamp(market)
        
        # Build content
        content = self.build_event_content(market)
        
        # Determine if market is open
        market_status = market.get("status", "unknown")
        is_open = market_status == "open"
        
        # Create event record
        event_data = {
            "content": content,
            "timestamp": timestamp,
            "verified": False,
            "metadata": {
                "kalshi_ticker": ticker,
                "source_url": KALSHI_SOURCE["url"],
                "category": "kalshi",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "question": question,
                "status": market_status,
                "is_open": is_open,
                "market_started": market.get("market_started"),
                "expiration": market.get("expiration"),
                "close_time": market.get("close_time"),
                "last_sale": market.get("last_sale"),
                "best_bid": market.get("best_bid"),
                "best_ask": market.get("best_ask"),
                "vol": market.get("vol"),
                "turnover": market.get("turnover"),
                "sub_market": market.get("sub_market"),
                "series_ticker": market.get("series_ticker"),
                "outcome": market.get("outcome"),
                "tags": market.get("tags", []),
            },
        }

        result = await self.db.create("event", event_data)
        return bool(result and len(result) > 0)

    async def fetch_markets(self, status: str = "open") -> List[dict]:
        """Fetch markets from Kalshi API."""
        markets = []
        cursor = None
        
        try:
            print(f"Fetching {status} markets from {KALSHI_API_MARKETS}...")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Build request with status filter
                params = {"limit": MAX_MARKETS_PER_RUN}
                if status:
                    params["status"] = status
                
                # First request
                response = await client.get(
                    KALSHI_API_MARKETS,
                    params=params,
                    headers={"Accept": "application/json"},
                )
                
                if response.status_code == 401:
                    print("Kalshi API requires authentication. Trying public endpoint...")
                    # Try without status filter for public data
                    response = await client.get(
                        KALSHI_API_MARKETS,
                        params={"limit": MAX_MARKETS_PER_RUN},
                        headers={"Accept": "application/json"},
                    )
                
                response.raise_for_status()
                data = response.json()
                
                # Handle pagination - Kalshi uses cursor-based pagination
                while True:
                    markets_batch = data.get("markets", [])
                    markets.extend(markets_batch)
                    
                    # Check for cursor for next page
                    cursor = data.get("cursor")
                    if not cursor:
                        break
                    
                    params["cursor"] = cursor
                    response = await client.get(
                        KALSHI_API_MARKETS,
                        params=params,
                        headers={"Accept": "application/json"},
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    # Safety limit
                    if len(markets) >= MAX_MARKETS_PER_RUN * 10:
                        print(f"Reached maximum market limit ({MAX_MARKETS_PER_RUN * 10})")
                        break
                
                print(f"Fetched {len(markets)} markets from API")
                
        except httpx.HTTPError as e:
            print(f"HTTP error fetching markets: {e}")
            self.stats["errors"].append({"stage": "fetch", "error": str(e)})
        except Exception as e:
            print(f"Error fetching markets: {e}")
            self.stats["errors"].append({"stage": "fetch", "error": str(e)})
        
        return markets[:MAX_MARKETS_PER_RUN]  # Return limited results

    async def process_markets(self, markets: List[dict], source_id: str) -> dict:
        """Process list of markets and ingest as events."""
        result = {
            "status": "success",
            "markets_processed": 0,
            "markets_filtered_quality": 0,
            "events_created": 0,
            "duplicates_skipped": 0,
            "errors": [],
            "filter_reasons": {},  # Track filter reasons for debugging
        }
        
        for market in markets:
            ticker = market.get("ticker", "unknown")
            
            # Apply quality filters (near-close, high-vol, liquidity, non-trivial)
            should_ingest, filter_reason = self.should_ingest_market(market)
            if not should_ingest:
                result["markets_filtered_quality"] += 1
                # Track filter reasons for debugging
                if filter_reason not in result["filter_reasons"]:
                    result["filter_reasons"][filter_reason] = 0
                result["filter_reasons"][filter_reason] += 1
                continue
            
            result["markets_processed"] += 1
            
            # Check for duplicate using ticker
            if await self.check_event_exists_by_ticker(ticker):
                result["duplicates_skipped"] += 1
                self.stats["duplicates_skipped"] += 1
                continue
            
            # Create event
            try:
                created = await self.create_event(market, source_id)
                if created:
                    result["events_created"] += 1
                    self.stats["events_created"] += 1
                    self.known_tickers.add(ticker)
                else:
                    result["errors"].append({"ticker": ticker, "error": "create failed"})
                    self.stats["errors"].append({"ticker": ticker, "error": "create failed"})
            except Exception as e:
                result["errors"].append({"ticker": ticker, "error": str(e)})
                self.stats["errors"].append({"ticker": ticker, "error": str(e)})
                print(f"Error creating event for {ticker}: {e}")
        
        self.stats["markets_fetched"] = len(markets)
        return result

    async def run(self) -> dict:
        """Execute the full Kalshi scraping pipeline."""
        print("=" * 60)
        print("Kalshi Scraper - Cycle6 Alt Markets Volva")
        print("=" * 60)
        print()

        # Connect to SurrealDB
        print("Connecting to SurrealDB...")
        await self.connect()
        print(f"Connected to {SURREALDB_NS}/{SURREALDB_DB}")
        print()

        # Load dedup tracker
        self.load_dedup_tracker()
        
        # Upsert source
        source_id = await self.upsert_source()
        print(f"Kalshi source ID: {source_id}")
        print()

        # Fetch open markets
        markets = await self.fetch_markets(status="open")
        
        if not markets:
            print("No markets fetched, trying without status filter...")
            markets = await self.fetch_markets(status=None)
        
        if not markets:
            print("No markets fetched from API, aborting.")
            await self.close()
            return {
                "run_id": datetime.now(timezone.utc).isoformat(),
                "status": "failed",
                "error": "No markets fetched from API",
                "stats": self.stats,
            }
        
        print()
        
        # Process markets
        result = await self.process_markets(markets, source_id)
        
        # Save dedup tracker
        self.save_dedup_tracker()
        print()

        # Summary
        print("-" * 60)
        print("RUN SUMMARY")
        print("-" * 60)
        print(f"Markets fetched: {self.stats['markets_fetched']}")
        print(f"Markets filtered (quality): {result.get('markets_filtered_quality', 0)}")
        if result.get('filter_reasons'):
            print("Filter reasons breakdown:")
            for reason, count in sorted(result['filter_reasons'].items(), key=lambda x: -x[1]):
                print(f"  - {reason}: {count}")
        print(f"Markets processed: {result.get('markets_processed', 0)}")
        print(f"Events created: {self.stats['events_created']}")
        print(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
        if self.stats['errors']:
            print(f"Errors: {len(self.stats['errors'])}")
        print("-" * 60)

        # Close connection
        await self.close()

        return {
            "run_id": datetime.now(timezone.utc).isoformat(),
            "source_id": source_id,
            "stats": self.stats,
            "result": result,
        }


async def main():
    """Entry point."""
    scraper = KalshiScraper()
    result = await scraper.run()
    
    # Output machine-parseable summary
    print()
    print("MACHINE_OUTPUT_START")
    print(json.dumps(result, indent=2))
    print("MACHINE_OUTPUT_END")
    
    return result


if __name__ == "__main__":
    asyncio.run(main())
