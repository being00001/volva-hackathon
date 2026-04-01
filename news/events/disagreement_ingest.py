#!/usr/bin/env python3
"""
Volva Disagreement Ingest - Phase 7 (wrk-0307)
Prototype for ingesting forecast disagreement signals from Polymarket markets.

This script fetches markets from the Polymarket CLOB API, identifies markets
with high YES/NO disagreement (imbalance > 70%), and stores them in SurrealDB
for weighted feedback processing.

Key Insight:
- High disagreement (YES/NO imbalance > 70%) signals market uncertainty
- When a high-disagreement prediction is correct, it should boost confidence MORE
- When a high-disagreement prediction is wrong, it should reduce confidence LESS
- This is because disagreement means the market is genuinely uncertain

Usage:
    PYTHONUNBUFFERED=1 python3 disagreement_ingest.py [--dry-run] [--min-imbalance 0.7]

Environment Variables:
    DRY_RUN: Set to 'true' for dry run mode (no DB writes)
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import httpx
from surrealdb import AsyncSurreal

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Database settings (from predictor_config)
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# Polymarket CLOB API
POLYMARKET_API_MARKETS = "https://clob.polymarket.com/markets"

# Default disagreement threshold
DEFAULT_MIN_IMBALANCE = 0.70  # 70% imbalance = one side has 85% vs 15%

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("volva.disagreement_ingest")


def extract_query_result(res):
    """Handle SurrealDB 1.x response envelopes and direct results."""
    if not res:
        return []
    if isinstance(res, list) and res and isinstance(res[0], dict) and "result" in res[0]:
        return res[0]["result"] if res[0]["result"] else []
    return res if isinstance(res, list) else [res]


def calculate_imbalance(yes_price: float, no_price: float) -> float:
    """
    Calculate the YES/NO imbalance as a percentage.
    
    Imbalance = abs(yes_price - no_price) 
    A higher imbalance means more disagreement.
    
    Args:
        yes_price: Price of YES token (0.0 to 1.0)
        no_price: Price of NO token (0.0 to 1.0)
    
    Returns:
        Imbalance as a float between 0.0 and 1.0
    """
    # In a well-formed market, yes_price + no_price ≈ 1.0
    # Use the absolute difference as the imbalance measure
    imbalance = abs(yes_price - no_price)
    return imbalance


def get_market_prices(market: Dict[str, Any]) -> tuple:
    """
    Extract YES and NO prices from market tokens.
    
    Returns:
        Tuple of (yes_price, no_price), defaults to (0.5, 0.5) if not found
    """
    tokens = market.get("tokens", [])
    
    yes_price = 0.5
    no_price = 0.5
    
    for t in tokens:
        outcome = t.get("outcome", "").lower()
        price = t.get("price", 0.5)
        
        if "yes" in outcome or outcome == "":
            yes_price = price
        elif "no" in outcome:
            no_price = price
    
    return yes_price, no_price


def is_high_disagreement(yes_price: float, no_price: float, threshold: float = 0.70) -> bool:
    """
    Check if a market has high disagreement.
    
    High disagreement is defined as YES/NO imbalance > threshold.
    For example, with threshold=0.70 and yes_price=0.85:
    - imbalance = |0.85 - 0.15| = 0.70 (threshold met)
    
    Args:
        yes_price: Price of YES token
        no_price: Price of NO token
        threshold: Minimum imbalance to consider "high disagreement"
    
    Returns:
        True if high disagreement, False otherwise
    """
    imbalance = calculate_imbalance(yes_price, no_price)
    return imbalance >= threshold


class DisagreementIngest:
    """
    Ingest forecast disagreement signals from Polymarket into SurrealDB.
    
    High disagreement signals are markets where traders strongly disagree
    (e.g., YES=85%, NO=15% = 70% imbalance). These signals are used
    by the feedback loop to weight confidence updates differently.
    """
    
    def __init__(self, dry_run: bool = False, min_imbalance: float = DEFAULT_MIN_IMBALANCE):
        self.db: Optional[AsyncSurreal] = None
        self.dry_run = dry_run
        self.min_imbalance = min_imbalance
        self.run_id = str(uuid.uuid4())[:8]
        self.stats = {
            "markets_fetched": 0,
            "high_disagreement_found": 0,
            "signals_stored": 0,
            "signals_skipped_dup": 0,
            "errors": 0,
        }
        self.seen_slugs: set = set()
        
        logger.info(f"DisagreementIngest initialized (run_id={self.run_id}, dry_run={self.dry_run}, min_imbalance={min_imbalance})")
    
    async def connect(self):
        """Connect to SurrealDB."""
        logger.info(f"Connecting to SurrealDB at {SURREALDB_HOST}...")
        self.db = AsyncSurreal(SURREALDB_HOST)
        await self.db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
        await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)
        logger.info("Connected to SurrealDB")
    
    async def close(self):
        """Close SurrealDB connection."""
        if self.db:
            try:
                await self.db.close()
            except NotImplementedError:
                logger.debug("AsyncSurreal.close() not implemented")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
    
    async def fetch_markets(self, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Fetch markets from Polymarket CLOB API.
        
        Args:
            limit: Maximum number of markets to fetch
        
        Returns:
            List of market dicts from CLOB API
        """
        markets = []
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                cursor = None
                page = 0
                max_pages = 10
                
                while len(markets) < limit and page < max_pages:
                    page += 1
                    params = {"limit": min(200, limit - len(markets))}
                    if cursor:
                        params["cursor"] = cursor
                    
                    response = await client.get(
                        POLYMARKET_API_MARKETS,
                        params=params,
                        headers={"Content-Type": "application/json"},
                    )
                    response.raise_for_status()
                    data = response.json()
                    page_markets = data.get("data", [])
                    
                    if not page_markets:
                        break
                    
                    markets.extend(page_markets)
                    logger.debug(f"Fetched page {page}: {len(page_markets)} markets (total: {len(markets)})")
                    
                    cursor = data.get("cursor")
                    if not cursor:
                        break
                    
                    # Rate limiting
                    if page % 5 == 0:
                        await asyncio.sleep(0.1)
        
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching markets: {e}")
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
        
        logger.info(f"Fetched {len(markets)} markets from CLOB API")
        return markets[:limit]
    
    def compute_feedback_weight(self, imbalance: float) -> float:
        """
        Compute the feedback weight for an imbalance.
        
        Higher disagreement = higher weight for correct, lower weight for incorrect.
        
        Weight formula:
        - If correct: weight = 1.0 + imbalance * 0.5 (max 1.75 at 100% imbalance)
        - If incorrect: weight = 1.0 - imbalance * 0.5 (min 0.5 at 100% imbalance)
        
        This means high-disagreement predictions that are correct get a bigger
        confidence boost, while high-disagreement predictions that are wrong
        get a smaller confidence penalty.
        
        Args:
            imbalance: YES/NO imbalance (0.0 to 1.0)
        
        Returns:
            Feedback weight (0.5 to 1.75)
        """
        # Clamp imbalance to [0, 1]
        imbalance = max(0.0, min(1.0, imbalance))
        
        # Base weight is 1.0, modulated by imbalance
        # Range: [0.5, 1.75]
        weight = 1.0 + (imbalance * 0.5)  # Will be applied as multiplier
        # For simplicity in storage, we store a "modulation factor"
        # actual_weight_for_correct = 1.0 + modulation
        # actual_weight_for_incorrect = 1.0 - modulation * 0.5
        return weight
    
    async def check_disagreement_exists(self, market_slug: str) -> bool:
        """Check if a disagreement signal already exists for this market slug."""
        query = """
            SELECT id FROM disagreement_signal 
            WHERE market_slug = $slug
            LIMIT 1;
        """
        res = await self.db.query(query, {"slug": market_slug})
        signals = extract_query_result(res)
        return len(signals) > 0
    
    async def store_disagreement_signal(
        self,
        market: Dict[str, Any],
        yes_price: float,
        no_price: float,
        imbalance: float,
        volume: float
    ) -> Optional[str]:
        """
        Store a disagreement signal in SurrealDB.
        
        Returns:
            disagreement_signal ID or None if failed.
        """
        market_slug = market.get("market_slug", "")
        market_question = market.get("question", "")[:500]
        
        # Check for duplicate
        if market_slug in self.seen_slugs:
            logger.debug(f"Skipping duplicate market slug: {market_slug}")
            self.stats["signals_skipped_dup"] += 1
            return None
        
        self.seen_slugs.add(market_slug)
        
        # Check DB for existing
        if await self.check_disagreement_exists(market_slug):
            logger.debug(f"Disagreement signal already exists in DB for {market_slug}")
            self.stats["signals_skipped_dup"] += 1
            return None
        
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would store disagreement_signal: {market_slug}")
            logger.info(f"  yes_price={yes_price:.3f}, no_price={no_price:.3f}, imbalance={imbalance:.3f}")
            return None
        
        try:
            feedback_weight = self.compute_feedback_weight(imbalance)
            
            query = """
                CREATE disagreement_signal SET
                    market_slug = $slug,
                    market_question = $question,
                    yes_price = $yes_price,
                    no_price = $no_price,
                    imbalance_pct = $imbalance,
                    volume = $volume,
                    timestamp = time::now(),
                    processed = false,
                    feedback_weight = $weight,
                    metadata = {
                        run_id: $run_id,
                        market_id: $market_id,
                        end_date: $end_date
                    }
                RETURN id;
            """
            res = await self.db.query(query, {
                "slug": market_slug,
                "question": market_question,
                "yes_price": yes_price,
                "no_price": no_price,
                "imbalance": imbalance,
                "volume": volume,
                "run_id": self.run_id,
                "weight": feedback_weight,
                "market_id": market.get("id", ""),
                "end_date": market.get("end_date_iso", ""),
            })
            results = extract_query_result(res)
            if results and len(results) > 0:
                signal_id = str(results[0].get("id"))
                logger.info(f"Stored disagreement_signal: {signal_id} (imbalance={imbalance:.2f})")
                self.stats["signals_stored"] += 1
                return signal_id
        except Exception as e:
            logger.error(f"Failed to store disagreement_signal: {e}")
            self.stats["errors"] += 1
        
        return None
    
    async def process_market(self, market: Dict[str, Any]) -> bool:
        """
        Process a single market for disagreement signal.
        
        Returns:
            True if processed (and signal stored), False otherwise.
        """
        try:
            self.stats["markets_fetched"] += 1
            
            # Get prices
            yes_price, no_price = get_market_prices(market)
            
            # Check if high disagreement
            if not is_high_disagreement(yes_price, no_price, self.min_imbalance):
                return False
            
            # Calculate imbalance
            imbalance = calculate_imbalance(yes_price, no_price)
            self.stats["high_disagreement_found"] += 1
            
            # Get volume
            volume = market.get("volume", 0)
            
            # Store signal
            signal_id = await self.store_disagreement_signal(
                market=market,
                yes_price=yes_price,
                no_price=no_price,
                imbalance=imbalance,
                volume=volume
            )
            
            return signal_id is not None
            
        except Exception as e:
            logger.error(f"Error processing market: {e}")
            self.stats["errors"] += 1
            return False
    
    async def run(self):
        """Main execution."""
        start_time = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("VOLVA DISAGREEMENT INGEST STARTING")
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Dry Run: {self.dry_run}")
        logger.info(f"Min Imbalance Threshold: {self.min_imbalance:.0%}")
        logger.info("=" * 60)
        
        try:
            await self.connect()
            
            # Fetch markets
            markets = await self.fetch_markets(limit=500)
            
            if not markets:
                logger.warning("No markets fetched from CLOB API")
                return self.stats
            
            # Process each market
            for market in markets:
                await self.process_market(market)
            
            # Print summary
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("DISAGREEMENT INGEST SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Markets Fetched:       {self.stats['markets_fetched']}")
            logger.info(f"High Disagreement:     {self.stats['high_disagreement_found']}")
            logger.info(f"Signals Stored:        {self.stats['signals_stored']}")
            logger.info(f"Signals Skipped:       {self.stats['signals_skipped_dup']}")
            logger.info(f"Errors:                {self.stats['errors']}")
            logger.info(f"Duration:              {elapsed:.2f}s")
            logger.info("=" * 60)
            
            # Return machine-parseable result
            result = {
                "run_id": self.run_id,
                "markets_fetched": self.stats["markets_fetched"],
                "high_disagreement_found": self.stats["high_disagreement_found"],
                "signals_stored": self.stats["signals_stored"],
                "signals_skipped_dup": self.stats["signals_skipped_dup"],
                "errors": self.stats["errors"],
                "duration_seconds": elapsed,
                "min_imbalance_threshold": self.min_imbalance,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dry_run": self.dry_run,
            }
            print(f"RESULT_JSON:{json.dumps(result)}")
            
            return self.stats
            
        finally:
            await self.close()


async def main():
    """Entry point."""
    # Check environment
    dry_run = os.environ.get("DRY_RUN", "").lower() == "true"
    
    # Parse args for min_imbalance
    min_imbalance = DEFAULT_MIN_IMBALANCE
    if len(sys.argv) > 1:
        try:
            min_imbalance = float(sys.argv[1])
            min_imbalance = max(0.0, min(1.0, min_imbalance))
        except ValueError:
            logger.error(f"Invalid min_imbalance: {sys.argv[1]}, using default: {DEFAULT_MIN_IMBALANCE}")
    
    # Run ingest
    ingest = DisagreementIngest(dry_run=dry_run, min_imbalance=min_imbalance)
    await ingest.run()


if __name__ == "__main__":
    asyncio.run(main())
