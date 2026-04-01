#!/usr/bin/env python3
"""
Manifold Scraper for Volva Cycle6 Alt Markets
Fetches prediction markets from Manifold API and ingests as events into SurrealDB.

API Endpoint: https://api.manifold.markets/v0/markets
Dedup: market id (unique per Manifold)
Category: manifold
Rate Limit: 500 requests/minute per IP (no auth required for reads)
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

# Manifold API Configuration
MANIFOLD_API_BASE = "https://api.manifold.markets"
MANIFOLD_API_MARKETS = f"{MANIFOLD_API_BASE}/v0/markets"
MAX_MARKETS_PER_RUN = 100  # Limit per ingestion run

# Market Quality Filter Configuration
CLOSE_WINDOW_DAYS = 7  # Only ingest markets closing within 7 days
MIN_VOLUME = 1000  # Minimum 24h volume for high-vol filter
MIN_LIQUIDITY = 100  # Minimum total liquidity
MIN_UNIQUE_BETTORS = 5  # Minimum unique bettors for engagement

# Trivial market exclusion: tags that indicate non-causal markets
TRIVIAL_TAGS = {
    "sports", "politics", "personal", "entertainment", "gambling",
    "celebrity", "music", "crypto", "nft", "gaming", "esports",
    "football", "basketball", "baseball", "soccer", "tennis",
    "election", "government", "law", "crime", "weather",
    "horoscope", "advice", "lifestyle", "food", "diet", "sleep",
    " IOM_", "shower", "toilet", "poop", "fart", "burp",
}

# Trivial question patterns (case-insensitive matching)
TRIVIAL_PATTERNS = [
    r"\bwill i\b", r"\bmy\b", r"\bi will\b", r"\bi'm going to\b",
    r"\bi wake up\b", r"\bi go to bed\b", r"\bi eat\b", r"\bi drink\b",
    r"\bwhat time\b", r"\bwhen will i\b", r"\bhow long will i\b",
    r"\bcoinflip\b", r"\bdaily question\b", r"\bpoll of\b",
    r"\bguess my\b", r"\bmy prediction\b", r"\bhow am i\b",
    r"\bwhat's my\b", r"\bwhat is my\b", r"\bmy chance\b",
    r"\bmy guess\b", r"\bI'M gonna\b", r"\bim gonna\b",
    r"\bi’m\b", r"\bgoing to bed\b", r"\bwake up\b",
    r"\bshould i\b", r"\bwill we\b", r"\bwill they\b",
    r"\bharry potter\b", r"\bstar wars\b", r"\bmovie\b",
    r"\bvideo game\b", r"\bgame of throne\b",
]

# Source info for Manifold
MANIFOLD_SOURCE = {
    "name": "Manifold Markets",
    "url": MANIFOLD_API_MARKETS,
    "type": "api",
    "reliability": 0.7,
    "categories": ["manifold", "prediction-market"],
}


class ManifoldScraper:
    """Scraper for Manifold prediction market data."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.stats = {
            "markets_fetched": 0,
            "events_created": 0,
            "duplicates_skipped": 0,
            "errors": [],
        }
        self.known_ids: set = set()
        self.dedup_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "manifold_dedup.json"
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
        """Load known market ids from persistent storage."""
        if os.path.exists(self.dedup_file):
            try:
                with open(self.dedup_file, "r") as f:
                    data = json.load(f)
                    self.known_ids = set(data.get("ids", []))
                    print(f"Loaded {len(self.known_ids)} known market ids")
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load dedup tracker: {e}")
                self.known_ids = set()

    def save_dedup_tracker(self):
        """Persist known market ids to storage."""
        try:
            with open(self.dedup_file, "w") as f:
                json.dump(
                    {
                        "ids": list(self.known_ids),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
        except IOError as e:
            print(f"Warning: Could not save dedup tracker: {e}")

    async def upsert_source(self) -> str:
        """Register or update Manifold source in source table."""
        url = MANIFOLD_SOURCE["url"]
        
        # Check if source exists
        result = await self.db.query(
            f"SELECT id FROM source WHERE url = '{url}' LIMIT 1;"
        )
        
        if result and len(result) > 0 and 'id' in result[0]:
            return str(result[0]['id'])

        # Create new source
        result = await self.db.create("source", {
            "url": url,
            "type": MANIFOLD_SOURCE["type"],
            "reliability": MANIFOLD_SOURCE["reliability"],
        })
        return str(result['id'])

    async def check_event_exists_by_id(self, market_id: str) -> bool:
        """Check if event with this market_id already exists."""
        if market_id in self.known_ids:
            return True
        
        # Also check database
        result = await self.db.query(
            f"SELECT id FROM event WHERE metadata.manifold_id = '{market_id}' LIMIT 1;"
        )
        if result and len(result) > 0 and 'id' in result[0]:
            self.known_ids.add(market_id)
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
        question_lower = question.lower()
        
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
        1. Near-close: closes within CLOSE_WINDOW_DAYS days
        2. High-vol: volume >= MIN_VOLUME or volume24Hours >= MIN_VOLUME
        3. Top liquidity: totalLiquidity >= MIN_LIQUIDITY
        4. Non-trivial: not in trivial categories
        
        Returns:
            (should_ingest, reason) - tuple of boolean and reason string
        """
        market_id = market.get("id", "unknown")
        
        # Filter 1: Check if already resolved (skip resolved markets)
        if market.get("isResolved", False):
            return False, "already_resolved"
        
        # Filter 2: Near-close filter - must close within 7 days
        close_time = market.get("closeTime")
        if close_time:
            try:
                if isinstance(close_time, (int, float)):
                    close_dt = datetime.fromtimestamp(close_time / 1000, tz=timezone.utc)
                elif isinstance(close_time, str):
                    close_dt_str = close_time.replace('Z', '+00:00')
                    close_dt = datetime.fromisoformat(close_dt_str)
                else:
                    close_dt = None
                
                if close_dt:
                    now = datetime.now(timezone.utc)
                    days_until_close = (close_dt - now).total_seconds() / 86400
                    if days_until_close > CLOSE_WINDOW_DAYS:
                        return False, f"too_far_out:{days_until_close:.1f}d"
                    if days_until_close < 0:
                        return False, "already_closed"
            except (ValueError, TypeError) as e:
                return False, f"timestamp_error:{e}"
        else:
            # No close time - skip (can't determine near-close)
            return False, "no_close_time"
        
        # Filter 3: High-vol filter - check volume or volume24Hours
        volume = market.get("volume", 0) or 0
        volume_24h = market.get("volume24Hours", 0) or 0
        if volume < MIN_VOLUME and volume_24h < MIN_VOLUME:
            return False, f"low_volume:vol={volume},vol24h={volume_24h}"
        
        # Filter 4: Top liquidity filter
        total_liquidity = market.get("totalLiquidity", 0) or 0
        if total_liquidity < MIN_LIQUIDITY:
            return False, f"low_liquidity:{total_liquidity}"
        
        # Filter 5: Unique bettors filter (engagement indicator)
        unique_bettors = market.get("uniqueBettorCount", 0) or 0
        if unique_bettors < MIN_UNIQUE_BETTORS:
            return False, f"low_engagement:{unique_bettors}bettors"
        
        # Filter 6: Non-trivial filter
        is_trivial, trivial_reason = self.is_trivial_market(market)
        if is_trivial:
            return False, trivial_reason
        
        return True, ""

    def parse_market_timestamp(self, market: dict) -> datetime:
        """Extract timestamp from market data (UNIX ms to datetime)."""
        # Priority: closeTime > createdTime
        ts = market.get("closeTime") or market.get("createdTime")
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
        
        # Get probability and pool info
        probability = market.get("probability", market.get("p", 0))
        pool = market.get("pool", {})
        
        # Clean description (remove newlines)
        if description:
            description = description.replace("\n", " ").strip()
        
        content = question
        if description:
            content += f" | {description}"
        if probability:
            content += f" | Probability: {probability:.2%}"
        
        return content[:2000]  # Truncate if too long

    async def create_event(self, market: dict, source_id: str) -> bool:
        """Create an event record in SurrealDB from market data."""
        market_id = market.get("id", "")
        question = market.get("question", "")
        
        # Parse timestamp
        timestamp = self.parse_market_timestamp(market)
        
        # Build content
        content = self.build_event_content(market)
        
        # Determine if market is open
        is_resolved = market.get("isResolved", False)
        is_open = not is_resolved
        
        # Extract probability
        probability = market.get("probability", market.get("p", 0))
        
        # Get pool info
        pool = market.get("pool", {})
        
        # Get outcomes if available
        outcome_type = market.get("outcomeType", "UNKNOWN")
        
        # Create event record
        event_data = {
            "content": content,
            "timestamp": timestamp,
            "verified": False,
            "metadata": {
                "manifold_id": market_id,
                "source_url": MANIFOLD_SOURCE["url"],
                "category": "manifold",
                "fetch_time": datetime.now(timezone.utc).isoformat(),
                "question": question,
                "is_resolved": is_resolved,
                "is_open": is_open,
                "close_time": market.get("closeTime"),
                "created_time": market.get("createdTime"),
                "probability": probability,
                "pool": pool,
                "volume": market.get("volume", 0),
                "volume_24h": market.get("volume24Hours", 0),
                "outcome_type": outcome_type,
                "mechanism": market.get("mechanism", ""),
                "slug": market.get("slug", ""),
                "url": market.get("url", ""),
                "creator_username": market.get("creatorUsername", ""),
                "unique_bettor_count": market.get("uniqueBettorCount", 0),
                "total_liquidity": market.get("totalLiquidity", 0),
                "tags": market.get("tags", []),
            },
        }

        result = await self.db.create("event", event_data)
        return bool(result and len(result) > 0)

    async def fetch_markets(self, include_closed: bool = False) -> List[dict]:
        """Fetch markets from Manifold API."""
        markets = []
        
        try:
            print(f"Fetching markets from {MANIFOLD_API_MARKETS}...")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Manifold API: client-side filtering using isResolved field
                params = {"limit": MAX_MARKETS_PER_RUN}
                
                response = await client.get(
                    MANIFOLD_API_MARKETS,
                    params=params,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                data = response.json()
                
                # Manifold returns array directly
                markets = data if isinstance(data, list) else []
                
                print(f"Fetched {len(markets)} markets from API")
                
        except httpx.HTTPError as e:
            print(f"HTTP error fetching markets: {e}")
            self.stats["errors"].append({"stage": "fetch", "error": str(e)})
        except Exception as e:
            print(f"Error fetching markets: {e}")
            self.stats["errors"].append({"stage": "fetch", "error": str(e)})
        
        return markets[:MAX_MARKETS_PER_RUN]  # Return limited results

    async def process_markets(self, markets: List[dict], source_id: str, include_closed: bool = False) -> dict:
        """Process list of markets and ingest as events."""
        result = {
            "status": "success",
            "markets_processed": 0,
            "markets_filtered_resolved": 0,
            "markets_filtered_quality": 0,
            "events_created": 0,
            "duplicates_skipped": 0,
            "errors": [],
            "filter_reasons": {},  # Track filter reasons for debugging
        }
        
        for market in markets:
            market_id = market.get("id", "unknown")
            
            # Filter out closed/resolved markets if not including them
            is_resolved = market.get("isResolved", False)
            if not include_closed and is_resolved:
                result["markets_filtered_resolved"] += 1
                continue
            
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
            
            # Check for duplicate using id
            if await self.check_event_exists_by_id(market_id):
                result["duplicates_skipped"] += 1
                self.stats["duplicates_skipped"] += 1
                continue
            
            # Create event
            try:
                created = await self.create_event(market, source_id)
                if created:
                    result["events_created"] += 1
                    self.stats["events_created"] += 1
                    self.known_ids.add(market_id)
                else:
                    result["errors"].append({"id": market_id, "error": "create failed"})
                    self.stats["errors"].append({"id": market_id, "error": "create failed"})
            except Exception as e:
                result["errors"].append({"id": market_id, "error": str(e)})
                self.stats["errors"].append({"id": market_id, "error": str(e)})
                print(f"Error creating event for {market_id}: {e}")
        
        self.stats["markets_fetched"] = len(markets)
        return result

    async def run(self) -> dict:
        """Execute the full Manifold scraping pipeline."""
        print("=" * 60)
        print("Manifold Scraper - Cycle6 Alt Markets Volva")
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
        print(f"Manifold source ID: {source_id}")
        print()

        # Fetch open markets
        markets = await self.fetch_markets(include_closed=False)
        
        if not markets:
            print("No open markets fetched, trying with closed markets included...")
            markets = await self.fetch_markets(include_closed=True)
        
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
        result = await self.process_markets(markets, source_id, include_closed=False)
        
        # Save dedup tracker
        self.save_dedup_tracker()
        print()

        # Summary
        print("-" * 60)
        print("RUN SUMMARY")
        print("-" * 60)
        print(f"Markets fetched: {self.stats['markets_fetched']}")
        print(f"Markets filtered (resolved): {result.get('markets_filtered_resolved', 0)}")
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
    scraper = ManifoldScraper()
    result = await scraper.run()
    
    # Output machine-parseable summary
    print()
    print("MACHINE_OUTPUT_START")
    print(json.dumps(result, indent=2))
    print("MACHINE_OUTPUT_END")
    
    return result


if __name__ == "__main__":
    asyncio.run(main())
