#!/usr/bin/env python3
"""
Volva Prediction Engine - Phase 2 (OPTIMIZED)
Analyzes events and causal links to generate predictions about future outcomes.
Uses sliding windows to find matching causal patterns and Gemini for prediction generation.

Optimizations in this version:
    - Batch existing prediction checks (single DB query).
    - Prioritize events and causal links (top-k, freshness, confidence).
    - Improved logging with explicit flushing and timing.
    - Python unbuffered logging via flush=True and PYTHONUNBUFFERED=1.

Usage:
    PYTHONUNBUFFERED=1 DRY_RUN=false python3 predictor.py

Environment Variables:
    DRY_RUN: Set to 'true' for dry run mode (no DB writes)
    GOOGLE_AIS_API_KEY: Gemini API key
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from surrealdb import AsyncSurreal

# Configure structured logging
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s.%(msecs)03d] %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("volva.predictor")

# Instrumentation: track timing for each phase
_instrumentation = {
    "phases": {},
    "db_queries": [],
    "api_calls": [],
}

# Add parent directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from predictor_config import (
    SURREALDB_HOST,
    SURREALDB_USER,
    SURREALDB_PASS,
    SURREALDB_NS,
    SURREALDB_DB,
    GEMINI_API_KEY_ENV,
    GEMINI_MODEL,
    GEMINI_API_BASE,
    GEMINI_MAX_TOKENS,
    PREDICTION_WINDOW_HOURS,
    PREDICTION_HORIZON_DAYS,
    MIN_CONFIDENCE_THRESHOLD,
    MAX_PREDICTIONS_PER_RUN,
    MAX_RETRIES,
    RETRY_BACKOFF,
    DRY_RUN as CONFIG_DRY_RUN,
    VERBOSE,
    AGENT_NAME,
    CATEGORY_INDICATOR_MAP,
    CATEGORY_KEYWORDS,
)

# Environment variable overrides
DRY_RUN = os.environ.get("DRY_RUN", str(CONFIG_DRY_RUN)).lower() == "true"

# httpx Timeout configuration: default=45s, connect=10s
# Fixes hang issues where connection hangs before any data is sent
# httpx.Timeout(default, connect=X) overrides connect timeout specifically
HTTPX_TIMEOUT = httpx.Timeout(45.0, connect=10.0)

# JSON extraction guard: max characters to scan for closing brace
# Prevents slow processing on malformed/malicious responses
MAX_JSON_SCAN_LENGTH = 50000


def log_checkpoint(name: str, elapsed: float = None):
    """Log a checkpoint with timing information and flush logs."""
    ts = datetime.now(timezone.utc).isoformat()
    if elapsed is not None:
        msg = f"[CHECKPOINT] {name} completed in {elapsed:.3f}s"
    else:
        msg = f"[CHECKPOINT] {name} reached at {ts}"
    logger.info(msg)
    # Explicit flush for unbuffered capture
    sys.stdout.flush()
    sys.stderr.flush()


class VolvaPredictor:
    """Prediction engine that generates future outcome predictions based on causal patterns."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.gemini_api_key: Optional[str] = None
        self.stats = {
            "events_loaded": 0,
            "causal_links_loaded": 0,
            "predictions_generated": 0,
            "predictions_written": 0,
            "predictions_skipped_dupe": 0,
            "predictions_failed": 0,
            "errors": [],
        }
        self.seen_prediction_keys: set = set()  # For deduplication within run
        self.existing_prediction_keys: set = set()  # For deduplication with DB
        self.event_content_cache: dict[str, str] = {}  # Bulk cached event contents
        self.latest_indicators: dict[
            str, dict
        ] = {}  # Cache for latest indicator observations
        self.timing = {
            "connect": 0.0,
            "load_events": 0.0,
            "load_links": 0.0,
            "load_existing": 0.0,
            "generate_predictions": 0.0,
            "write_predictions": 0.0,
            "total": 0.0,
        }
        logger.debug("VolvaPredictor initialized")

    async def connect(self):
        """Connect to SurrealDB."""
        start_time = time.perf_counter()
        logger.info("Connecting to SurrealDB at %s", SURREALDB_HOST)
        try:
            self.db = AsyncSurreal(SURREALDB_HOST)
            await self.db.signin(
                {"username": SURREALDB_USER, "password": SURREALDB_PASS}
            )
            await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)
            elapsed = time.perf_counter() - start_time
            self.timing["connect"] = elapsed
            logger.info(
                "Connected to %s/%s in %.3fs", SURREALDB_NS, SURREALDB_DB, elapsed
            )
            log_checkpoint("connect", elapsed)
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            logger.error("Failed to connect to SurrealDB after %.3fs: %s", elapsed, e)
            raise

    async def close(self):
        """Close SurrealDB connection."""
        if self.db:
            try:
                await self.db.close()
                logger.debug("SurrealDB connection closed successfully")
            except NotImplementedError:
                # Known issue with some surrealdb versions - connection may leak
                logger.warning(
                    "SurrealDB.close() not implemented - connection may not be properly released"
                )
            except Exception as e:
                logger.warning("Error closing SurrealDB connection: %s", e)
            finally:
                self.db = None

    def get_gemini_api_key(self) -> str:
        """Get Gemini API key from environment."""
        key = os.environ.get(GEMINI_API_KEY_ENV)
        if not key:
            raise ValueError(f"{GEMINI_API_KEY_ENV} environment variable not set")
        return key

    async def load_recent_events(self) -> list[dict]:
        """Load recent events within the prediction window."""
        start_time = time.perf_counter()
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            hours=PREDICTION_WINDOW_HOURS
        )
        cutoff_str = cutoff_time.isoformat()

        query = """
            SELECT * FROM event 
            WHERE timestamp >= $cutoff
            ORDER BY timestamp DESC;
        """
        logger.debug("Executing query with cutoff: %s", cutoff_str)
        try:
            db_start = time.perf_counter()
            result = await self.db.query(query, {"cutoff": cutoff_str})
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "load_recent_events", "elapsed_ms": db_elapsed * 1000}
            )
            logger.debug("DB query completed in %.3fs", db_elapsed)

            events = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    events = result[0]["result"]
                else:
                    events = result

            events = [e for e in events if e.get("content")]
            elapsed = time.perf_counter() - start_time
            self.timing["load_events"] = elapsed
            self.stats["events_loaded"] = len(events)
            logger.info(
                "Loaded %d recent events (from %dh window) in %.3fs",
                len(events),
                PREDICTION_WINDOW_HOURS,
                elapsed,
            )
            log_checkpoint("load_recent_events", elapsed)
            return events
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            logger.error("Error loading events after %.3fs: %s", elapsed, e)
            raise

    async def load_causal_links(self) -> list[dict]:
        """Load high-confidence causal links."""
        start_time = time.perf_counter()
        query = f"""
            SELECT * FROM causal_link 
            WHERE confidence >= {MIN_CONFIDENCE_THRESHOLD}
            ORDER BY confidence DESC;
        """
        logger.debug("Executing query: %s", query.strip())
        try:
            db_start = time.perf_counter()
            result = await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "load_causal_links", "elapsed_ms": db_elapsed * 1000}
            )
            logger.debug("DB query completed in %.3fs", db_elapsed)

            links = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    links = result[0]["result"]
                else:
                    links = result

            elapsed = time.perf_counter() - start_time
            self.timing["load_links"] = elapsed
            self.stats["causal_links_loaded"] = len(links)
            logger.info(
                "Loaded %d causal links (confidence >= %.2f) in %.3fs",
                len(links),
                MIN_CONFIDENCE_THRESHOLD,
                elapsed,
            )
            log_checkpoint("load_causal_links", elapsed)
            return links
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            logger.error("Error loading causal links after %.3fs: %s", elapsed, e)
            raise

    async def load_existing_predictions(self):
        """Batch load existing predictions to avoid O(N*M) DB checks."""
        start_time = time.perf_counter()
        logger.info("Batch loading existing predictions from DB...")
        query = "SELECT source_event, causal_link_id FROM predicts;"
        try:
            db_start = time.perf_counter()
            result = await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "load_existing_predictions", "elapsed_ms": db_elapsed * 1000}
            )

            items = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    items = result[0]["result"]
                else:
                    items = result

            for item in items:
                source_id = str(item.get("source_event", ""))
                link_id = str(item.get("causal_link_id", ""))
                if source_id and link_id:
                    self.existing_prediction_keys.add(f"{source_id}->{link_id}")

            elapsed = time.perf_counter() - start_time
            self.timing["load_existing"] = elapsed
            logger.info(
                "Loaded %d existing prediction keys in %.3fs",
                len(self.existing_prediction_keys),
                elapsed,
            )
            log_checkpoint("load_existing_predictions", elapsed)
        except Exception as e:
            logger.warning("Failed to batch load existing predictions: %s", e)

    async def bulk_cache_event_contents(
        self, events: list[dict], causal_links: list[dict]
    ):
        """Bulk fetch all event contents in a single DB query to avoid N+1 query problem."""
        start_time = time.perf_counter()

        # Collect unique event IDs needed
        event_ids = set()
        for e in events:
            event_ids.add(str(e.get("id", "")))
        for link in causal_links:
            event_ids.add(str(link.get("to", "")))

        # Filter out empty IDs
        event_ids = {eid for eid in event_ids if eid}

        if not event_ids:
            logger.info("No event IDs to cache")
            return

        logger.info("Bulk caching contents for %d events...", len(event_ids))

        # Single query with ALL IDs using SurrealDB array syntax for IN clause (unquoted record IDs)
        ids_list = "[" + ", ".join(sorted(event_ids)) + "]"
        query = f"SELECT id, content FROM event WHERE id IN {ids_list};"

        try:
            db_start = time.perf_counter()
            result = await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "bulk_cache_event_contents", "elapsed_ms": db_elapsed * 1000}
            )

            items = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    items = result[0]["result"]
                else:
                    items = result

            cached_count = 0
            for item in items:
                eid = str(item.get("id", ""))
                content = item.get("content", "")
                if eid and content:
                    self.event_content_cache[eid] = content
                    cached_count += 1

            elapsed = time.perf_counter() - start_time
            logger.info(
                "Cached %d/%d event contents in %.3fs",
                cached_count,
                len(event_ids),
                elapsed,
            )
            log_checkpoint("bulk_cache_event_contents", elapsed)
        except Exception as e:
            logger.warning("Failed to bulk cache event contents: %s", e)

    async def load_latest_indicators(self) -> dict[str, dict]:
        """Load the most recent observation for each indicator series and cache."""
        start_time = time.perf_counter()

        # Query: Get latest observation per series using GROUP BY with max date
        query = """
            SELECT series_id, date, value,
                   ->indicator_series.name AS series_name,
                   ->indicator_series.unit AS unit
            FROM indicator_observation
            GROUP BY series_id
        """

        try:
            result = await self.db.query(query)
            indicators = {}

            if isinstance(result, list) and result:
                items = result[0].get("result", []) if "result" in result[0] else result
                for item in items:
                    series_id = str(item.get("series_id", ""))
                    indicators[series_id] = {
                        "date": item.get("date"),
                        "value": item.get("value"),
                        "unit": item.get("unit", ""),
                        "series_name": item.get("series_name", ""),
                    }

            self.latest_indicators = indicators  # Cache on self
            elapsed = time.perf_counter() - start_time
            logger.info(
                "Loaded %d latest indicator observations in %.3fs",
                len(indicators),
                elapsed,
            )
            log_checkpoint("load_latest_indicators", elapsed)
            return indicators
        except Exception as e:
            logger.warning("Failed to load latest indicators: %s", e)
            return {}

    def parse_timestamp(self, ts) -> Optional[datetime]:
        """Robust timestamp parsing."""
        if not ts:
            return None
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def detect_news_categories(self, content: str) -> list[str]:
        """
        Detect relevant news categories from event content using keyword matching.

        Args:
            content: The event content string to analyze

        Returns:
            List of matching category names (can be empty)
        """
        if not content:
            return []

        content_lower = content.lower()
        matched_categories = []

        for category, keywords in CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in content_lower:
                    if category not in matched_categories:
                        matched_categories.append(category)
                    break  # Found a match for this category, no need to check more keywords

        logger.debug(
            "Detected categories for content: %s -> %s",
            content[:50] if content else "",
            matched_categories,
        )
        return matched_categories

    async def load_indicators_for_categories(
        self, categories: list[str]
    ) -> dict[str, dict]:
        """
        Load latest indicator observations for specific categories only.

        Args:
            categories: List of category names to load indicators for

        Returns:
            Dict mapping series_id -> {date, value, unit, series_name}
        """
        if not categories:
            return {}

        # Resolve categories to indicator series IDs
        series_ids = []
        for category in categories:
            if category in CATEGORY_INDICATOR_MAP:
                series_ids.extend(CATEGORY_INDICATOR_MAP[category])

        if not series_ids:
            return {}

        # Deduplicate and limit to avoid excessive queries
        series_ids = list(set(series_ids))[:20]

        logger.info(
            "Loading indicators for categories %s (series: %s)", categories, series_ids
        )

        # Build query with series_id filter
        # SurrealDB uses record IDs like "indicator_series:xxx" for -> links
        series_filter = "[" + ", ".join([f'"{sid}"' for sid in series_ids]) + "]"
        query = f"""
            SELECT series_id, date, value,
                   ->indicator_series.name AS series_name,
                   ->indicator_series.unit AS unit
            FROM indicator_observation
            WHERE series_id IN {series_filter}
            GROUP BY series_id
        """

        try:
            db_start = time.perf_counter()
            result = await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {
                    "query": "load_indicators_for_categories",
                    "elapsed_ms": db_elapsed * 1000,
                    "categories": categories,
                }
            )

            indicators = {}
            if isinstance(result, list) and result:
                items = result[0].get("result", []) if "result" in result[0] else result
                for item in items:
                    series_id = str(item.get("series_id", ""))
                    indicators[series_id] = {
                        "date": item.get("date"),
                        "value": item.get("value"),
                        "unit": item.get("unit", ""),
                        "series_name": item.get("series_name", ""),
                    }

            logger.info(
                "Loaded %d category-specific indicator observations", len(indicators)
            )
            return indicators
        except Exception as e:
            logger.warning(
                "Failed to load indicators for categories %s: %s", categories, e
            )
            return {}

    def get_prediction_key(self, source_event_id: str, causal_link_id: str) -> str:
        """Generate deterministic deduplication key for a prediction."""
        return f"{source_event_id}->{causal_link_id}"

    def rank_prediction_candidate(
        self,
        trigger_event: dict,
        causal_link: dict,
        category_indicators: dict[str, dict] = None,
    ) -> float:
        """
        Compute a ranking score for a prediction candidate.

        This implements ranking: higher scores indicate predictions more likely
        to be accurate, allowing us to prioritize high-quality predictions.

        Scoring factors:
        - Causal link confidence: weight 0.5
        - Mechanism strength: based on mechanism length and specificity (0.0-0.3)
        - Category match: indicator match bonus (0.0-0.2)

        Returns:
            Float score in range [0.0, 1.0]
        """
        score = 0.0

        # 1. Causal link confidence (weight 0.5)
        link_confidence = causal_link.get("confidence", 0.0)
        score += link_confidence * 0.5

        # 2. Mechanism strength (weight 0.3)
        # Longer mechanisms often indicate more specific causal reasoning
        mechanism = causal_link.get("mechanism", "")
        if mechanism and len(mechanism) > 20:
            # Score based on mechanism specificity (capped at 0.3)
            mech_score = min(0.3, len(mechanism) / 500)
            score += mech_score

        # 3. Category match bonus (weight 0.2)
        if category_indicators:
            event_content = trigger_event.get("content", "").lower()
            # Check if trigger event relates to the indicators we have data for
            for series_id, data in category_indicators.items():
                series_name = data.get("series_name", "").lower()
                if series_name and series_name in event_content:
                    score += 0.1
                    break

        # 4. Freshness bonus (0.0-0.1) - more recent events score higher
        ts = self.parse_timestamp(trigger_event.get("timestamp"))
        if ts:
            hours_old = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if hours_old < 6:
                score += 0.1
            elif hours_old < 24:
                score += 0.05

        return min(1.0, score)

    async def get_event_content(self, event_id: str) -> Optional[str]:
        """Fetch event content by ID."""
        query = f"SELECT content FROM event WHERE id = {event_id} LIMIT 1;"
        try:
            db_start = time.perf_counter()
            result = await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "get_event_content", "elapsed_ms": db_elapsed * 1000}
            )

            if isinstance(result, list) and result:
                if isinstance(result[0], dict) and "result" in result[0]:
                    items = result[0]["result"]
                else:
                    items = result
                if items:
                    return items[0].get("content", "")
            return None
        except Exception as e:
            logger.warning("get_event_content(%s) failed: %s", event_id[:20], e)
            return None

    async def generate_prediction_with_gemini(
        self,
        trigger_event: dict,
        causal_link: dict,
        client: httpx.AsyncClient,
        category_indicators: dict[str, dict] = None,
    ) -> dict:
        """Query Gemini to generate a prediction based on causal pattern.

        Args:
            trigger_event: The event that triggered this prediction
            causal_link: The causal link pattern to use
            client: httpx AsyncClient for API calls
            category_indicators: Dict of series_id -> {date, value, unit, series_name}
                               for indicators relevant to the trigger event's category
        """

        # Get the target event pattern from the causal link (use cache)
        target_event_id = str(causal_link.get("to", ""))
        target_content = self.event_content_cache.get(target_event_id, "")

        # Get mechanism and confidence from causal link
        mechanism = causal_link.get("mechanism", "unknown")
        link_confidence = causal_link.get("confidence", 0.5)

        # Build category indicator context (from trigger event's detected categories)
        category_indicator_context = ""
        if category_indicators:
            indicator_lines = []
            for series_id, data in category_indicators.items():
                series_name = data.get("series_name", series_id)
                value = data.get("value", "N/A")
                unit = data.get("unit", "")
                date = data.get("date", "N/A")
                indicator_lines.append(f"- {series_name}: {value} {unit} ({date})")

            if indicator_lines:
                category_indicator_context = """
RELATED ECONOMIC INDICATORS (relevant to this news category):
""" + "\n".join(indicator_lines)

        # Check if target is an indicator_series and get current indicator state
        target_is_indicator = False
        indicator_context = ""
        indicator_spec_prompt = ""
        if target_event_id and target_event_id.startswith("indicator_series:"):
            target_is_indicator = True
            indicator_data = self.latest_indicators.get(target_event_id, {})
            if indicator_data:
                value = indicator_data.get("value", "N/A")
                date = indicator_data.get("date", "N/A")
                unit = indicator_data.get("unit", "")
                series_name = indicator_data.get("series_name", "")
                indicator_context = f"""
CURRENT INDICATOR STATE:
- Indicator: {series_name}
- Latest Value: {value} {unit}
- As of: {date}
"""
                indicator_spec_prompt = f"""
When the causal link target is an indicator series ({series_name}), 
predict the SPECIFIC numerical value or direction of change you expect.
Reference the current value ({value} {unit}) in your prediction.
Example: "Fed Funds Rate will rise from {value}% to X%" or "Unemployment will decrease by Y percentage points"
"""

        prompt = f"""You are a causal prediction expert for a prediction market. Given a causal pattern, generate a specific prediction.

CAUSAL PATTERN:
- Cause event: {trigger_event.get("content", "")[:500]}
{category_indicator_context}{indicator_context}- Known effect pattern: {target_content[:500] if target_content else "similar events"}
- Mechanism: {mechanism}
- Historical confidence: {link_confidence:.2f}
{indicator_spec_prompt}TASK:
Generate a specific, falsifiable prediction about what will happen next, based on this causal pattern.{" Include the current indicator values in your prediction." if (category_indicator_context or indicator_context) else ""}

Return ONLY valid JSON:
{{
    "target_description": "What specifically will happen (be concrete and falsifiable){", referencing current indicator values" if (category_indicator_context or indicator_context) else ""}",
    "probability": 0.0-1.0 (estimate based on mechanism strength and historical confidence),
    "rationale": "2-3 sentence explanation of why this prediction follows from the pattern",
    "target_date_max": "ISO datetime when this prediction should be verified by (14 days from now)"
}}

Constraints:
- Predictions should be verifiable within the next 14 days
- Be specific about the outcome, not vague
- Probability should reflect both the link confidence and your assessment of timeliness
- When predicting indicator values, provide specific numbers with units

Response:"""

        url = f"{GEMINI_API_BASE}/{GEMINI_MODEL}:generateContent"
        params = {"key": self.gemini_api_key}
        headers = {"Content-Type": "application/json"}
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": GEMINI_MAX_TOKENS,
                "temperature": 0.4,
            },
        }

        for attempt in range(MAX_RETRIES):
            attempt_start = time.perf_counter()
            try:
                logger.debug(
                    "Gemini API call attempt %d/%d to %s",
                    attempt + 1,
                    MAX_RETRIES,
                    GEMINI_MODEL,
                )
                response = await client.post(
                    url,
                    json=body,
                    headers=headers,
                    params=params,
                    timeout=HTTPX_TIMEOUT,
                )
                api_elapsed = time.perf_counter() - attempt_start
                _instrumentation["api_calls"].append(
                    {
                        "model": GEMINI_MODEL,
                        "attempt": attempt + 1,
                        "elapsed_ms": api_elapsed * 1000,
                        "status": response.status_code,
                    }
                )

                if response.status_code == 429:
                    wait_time = RETRY_BACKOFF * (2**attempt)
                    logger.warning(
                        "Rate limited on attempt %d, waiting %.1fs",
                        attempt + 1,
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    continue

                if response.status_code != 200:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                    }

                logger.debug("Gemini API responded in %.3fs", api_elapsed)
                try:
                    data = response.json()
                except (json.JSONDecodeError, httpx.DecodingError) as e:
                    # Handle malformed JSON responses (e.g., truncated gzip streams)
                    return {
                        "success": False,
                        "error": f"Response JSON decode error: {e}",
                    }

                # Extract text from response
                candidates = data.get("candidates", [])
                if not candidates:
                    return {"success": False, "error": "No candidates in response"}

                candidate = candidates[0]
                finish_reason = candidate.get("finishReason", "UNKNOWN")

                if finish_reason not in ("STOP", "MAX_TOKENS"):
                    return {
                        "success": False,
                        "error": f"Finish reason: {finish_reason}",
                    }

                content = candidate.get("content", {})
                parts = content.get("parts", [])
                text = ""
                for part in parts:
                    if "text" in part:
                        text = part["text"]
                        break

                if not text:
                    return {
                        "success": False,
                        "error": "No text in response",
                        "finish_reason": finish_reason,
                    }

                # Parse JSON response
                try:
                    text = text.strip()
                    if text.startswith("```"):
                        lines = text.split("\n")
                        if len(lines) >= 2:
                            text = "\n".join(lines[1:])
                        if text.endswith("```"):
                            text = text[:-3]
                        text = text.strip()

                    try:
                        result = json.loads(text)
                    except json.JSONDecodeError:
                        json_start = text.find("{")
                        if json_start < 0:
                            raise ValueError("No JSON object found in text")

                        depth = 0
                        json_end = json_start
                        in_string = False
                        escape_next = False

                        for i, char in enumerate(text[json_start:], start=json_start):
                            # Guard against extremely long malformed responses
                            if i - json_start > MAX_JSON_SCAN_LENGTH:
                                raise ValueError(
                                    f"JSON scan exceeded {MAX_JSON_SCAN_LENGTH} characters"
                                )

                            if escape_next:
                                escape_next = False
                                continue
                            if char == "\\":
                                escape_next = True
                                continue
                            if char == '"' and not escape_next:
                                in_string = not in_string
                                continue
                            if in_string:
                                continue
                            if char == "{":
                                depth += 1
                            elif char == "}":
                                depth -= 1
                                if depth == 0:
                                    json_end = i + 1
                                    break

                        # If we exited loop but still in string, the JSON is truncated
                        # Try to use what we have and patch the string issue
                        if in_string and json_end > json_start:
                            # The truncated string likely ends mid-value
                            # Try parsing what we have - if it fails, try to patch
                            json_str = text[json_start:json_end]
                            try:
                                result = json.loads(json_str)
                            except json.JSONDecodeError:
                                # Last resort: try to find complete values before truncation
                                # Look for the last complete field by finding '"...":' patterns
                                last_complete_pos = text.rfind(
                                    '",\n    "', json_start, json_end
                                )
                                if last_complete_pos > json_start:
                                    # Try parsing up to after that field
                                    patch_end = text.find("}", last_complete_pos + 1)
                                    if patch_end > 0:
                                        json_str = text[json_start : patch_end + 1]
                                        try:
                                            result = json.loads(json_str)
                                        except json.JSONDecodeError:
                                            raise ValueError(
                                                f"Truncated JSON even after patch attempt"
                                            )
                                    else:
                                        raise ValueError(
                                            "Cannot find closing brace after last complete field"
                                        )
                                else:
                                    raise ValueError(
                                        "Truncated JSON - no complete fields found"
                                    )
                        else:
                            json_str = text[json_start:json_end]
                            result = json.loads(json_str)

                    return {
                        "success": True,
                        "target_description": result.get("target_description", ""),
                        "probability": float(result.get("probability", 0.0)),
                        "rationale": result.get("rationale", ""),
                        "target_date_max": result.get("target_date_max", ""),
                    }
                except (json.JSONDecodeError, ValueError) as e:
                    raw_preview = text[:500] if len(text) > 500 else text
                    return {
                        "success": False,
                        "error": f"JSON parse error: {e}, raw: {repr(raw_preview)}",
                    }

            except (asyncio.TimeoutError, httpx.TimeoutException):
                if attempt < MAX_RETRIES - 1:
                    logger.warning("Timeout on attempt %d, retrying...", attempt + 1)
                    continue
                return {"success": False, "error": "Request timeout after retries"}
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
                    continue
                return {"success": False, "error": str(e)}

        return {"success": False, "error": "Max retries exceeded"}

    async def write_prediction(
        self,
        source_event_id: str,
        causal_link_id: str,
        target_description: str,
        probability: float,
        rationale: str,
        target_date_max: str,
    ) -> bool:
        """Write a prediction to SurrealDB."""
        clean_target = target_description.replace('"', '\\"').replace("\n", " ")[:500]
        clean_rationale = rationale.replace('"', '\\"').replace("\n", " ")[:1000]

        # FIX (wrk-0256): Ensure proper SurrealDB record ID format for source_event
        # Record IDs should be in format 'table:id' - add 'event:' prefix if missing
        formatted_source_event = source_event_id
        if not formatted_source_event.startswith("event:"):
            formatted_source_event = f"event:{formatted_source_event}"

        # Similarly ensure causal_link_id has proper prefix
        formatted_causal_link = causal_link_id
        if not formatted_causal_link.startswith("causal_link:"):
            formatted_causal_link = f"causal_link:{formatted_causal_link}"

        query = f"""
            CREATE predicts SET
            agent = "{AGENT_NAME}",
            source_event = {formatted_source_event},
            causal_link_id = {formatted_causal_link},
            target_description = "{clean_target}",
            probability = {probability},
            rationale = "{clean_rationale}",
            status = "pending",
            evidence_event = NONE,
            timestamp = time::now(),
            target_date_max = time::now() + 14d;
        """

        try:
            db_start = time.perf_counter()
            await self.db.query(query)
            db_elapsed = time.perf_counter() - db_start
            _instrumentation["db_queries"].append(
                {"query": "write_prediction", "elapsed_ms": db_elapsed * 1000}
            )
            return True
        except Exception as e:
            logger.error(
                "DB write error for prediction %s -> %s: %s",
                source_event_id[:20],
                causal_link_id[:20],
                e,
            )
            return False

    async def generate_predictions(
        self,
        recent_events: list[dict],
        causal_links: list[dict],
    ) -> list[dict]:
        """Generate predictions by matching events against causal patterns.

        This implementation uses RANKING to prioritize high-quality prediction
        candidates before Gemini API calls, improving hit rate by focusing on
        predictions most likely to be correct.
        """
        start_time = time.perf_counter()

        if not recent_events or not causal_links:
            logger.warning("No events or causal links to process")
            return []

        logger.info(
            "Starting prediction generation: %d events x %d causal links (max pairs %d)",
            len(recent_events),
            len(causal_links),
            len(recent_events) * len(causal_links),
        )
        predictions = []

        self.gemini_api_key = self.get_gemini_api_key()

        # RANKING: Build candidate list with pre-computed ranks before Gemini calls
        # This allows us to process highest-ranked pairs first
        logger.info(
            "[RANKING] Computing ranks for %d x %d = %d candidate pairs...",
            len(recent_events),
            len(causal_links),
            len(recent_events) * len(causal_links),
        )

        # Build all candidate pairs with their ranking scores
        candidates_with_ranks = []
        for event_idx, event in enumerate(recent_events):
            event_id = str(event.get("id", ""))
            event_content = event.get("content", "")

            # Pre-detect categories for this event
            detected_categories = self.detect_news_categories(event_content)
            category_indicators = {}
            if detected_categories:
                category_indicators = await self.load_indicators_for_categories(
                    detected_categories
                )

            for link_idx, link in enumerate(causal_links):
                link_id = str(link.get("id", ""))

                # Check for duplicate within run or DB
                pred_key = self.get_prediction_key(event_id, link_id)
                if pred_key in self.seen_prediction_keys:
                    continue

                if not DRY_RUN and pred_key in self.existing_prediction_keys:
                    continue

                # Compute ranking score for this candidate
                rank_score = self.rank_prediction_candidate(
                    event, link, category_indicators
                )

                candidates_with_ranks.append(
                    {
                        "event": event,
                        "link": link,
                        "event_idx": event_idx,
                        "link_idx": link_idx,
                        "rank_score": rank_score,
                        "category_indicators": category_indicators,
                        "pred_key": pred_key,
                    }
                )

        # Sort by rank score descending (highest quality predictions first)
        candidates_with_ranks.sort(key=lambda x: x["rank_score"], reverse=True)

        logger.info(
            "[RANKING] Sorted %d candidates by rank. Top 5 scores: %s",
            len(candidates_with_ranks),
            [f"{c['rank_score']:.3f}" for c in candidates_with_ranks[:5]],
        )

        # Track seen keys as we process
        processed_count = 0

        async with httpx.AsyncClient() as client:
            for candidate in candidates_with_ranks:
                if len(predictions) >= MAX_PREDICTIONS_PER_RUN:
                    break

                event = candidate["event"]
                link = candidate["link"]
                event_idx = candidate["event_idx"]
                link_idx = candidate["link_idx"]
                category_indicators = candidate["category_indicators"]
                pred_key = candidate["pred_key"]

                event_id = str(event.get("id", ""))
                link_id = str(link.get("id", ""))

                # Mark as seen
                self.seen_prediction_keys.add(pred_key)
                processed_count += 1

                # Generate prediction using Gemini (with category indicators)
                logger.info(
                    "Processing ranked candidate %d/%d (rank=%.3f): event %d/%d with link %d/%d: %s... | %s...",
                    processed_count,
                    len(candidates_with_ranks),
                    candidate["rank_score"],
                    event_idx + 1,
                    len(recent_events),
                    link_idx + 1,
                    len(causal_links),
                    event_id[:15],
                    link_id[:15],
                )
                sys.stdout.flush()

                result = await self.generate_prediction_with_gemini(
                    event, link, client, category_indicators
                )

                # FIX (wrk-0256): Defensive check - ensure result is a dict before calling .get()
                # This handles the 'str' object has no attribute 'get' error
                if not isinstance(result, dict):
                    logger.warning(
                        "Prediction failed for %s -> %s: result is not a dict, got %s",
                        event_id[:20],
                        link_id[:20],
                        type(result).__name__,
                    )
                    self.stats["predictions_failed"] += 1
                    continue

                if not result.get("success"):
                    logger.warning(
                        "Prediction failed for %s -> %s: %s",
                        event_id[:20],
                        link_id[:20],
                        result.get("error"),
                    )
                    self.stats["predictions_failed"] += 1
                    continue

                pred = {
                    "source_event": event_id,
                    "causal_link_id": link_id,
                    "target_description": result.get("target_description", ""),
                    "probability": result.get("probability", 0.0),
                    "rationale": result.get("rationale", ""),
                    "target_date_max": result.get("target_date_max", ""),
                }
                predictions.append(pred)
                self.stats["predictions_generated"] += 1

                logger.info(
                    "Generated prediction: %s (p=%.2f)",
                    pred["target_description"][:70],
                    pred["probability"],
                )

                # Rate limiting
                await asyncio.sleep(0.5)

        elapsed = time.perf_counter() - start_time
        self.timing["generate_predictions"] = elapsed
        logger.info(
            "Prediction generation completed: %d predictions in %.3fs",
            len(predictions),
            elapsed,
        )
        log_checkpoint("generate_predictions", elapsed)

        return predictions

    async def run(self) -> dict:
        """Execute the full prediction generation pipeline."""
        total_start = time.perf_counter()
        logger.info("=" * 60)
        logger.info("Volva Prediction Engine - Phase 2 (OPTIMIZED)")
        logger.info("=" * 60)
        logger.info("Mode: %s", "DRY_RUN" if DRY_RUN else "PRODUCTION")
        logger.info("Max predictions per run: %d", MAX_PREDICTIONS_PER_RUN)

        await self.connect()

        try:
            # Load data
            recent_events = await self.load_recent_events()
            causal_links = await self.load_causal_links()

            # Load latest indicators for prediction context
            await self.load_latest_indicators()

            if not DRY_RUN:
                await self.load_existing_predictions()

            if not recent_events:
                logger.warning("No recent events to process")
                return {"stats": self.stats, "predictions": []}

            if not causal_links:
                logger.warning("No causal links found")
                return {"stats": self.stats, "predictions": []}

            # Bulk cache event contents to avoid N+1 queries during prediction generation
            await self.bulk_cache_event_contents(recent_events, causal_links)

            # Generate predictions
            predictions = await self.generate_predictions(recent_events, causal_links)

            # Write predictions to DB
            write_start = time.perf_counter()
            if not DRY_RUN and predictions:
                logger.info("Writing %d predictions to database...", len(predictions))
                for pred in predictions:
                    success = await self.write_prediction(
                        pred["source_event"],
                        pred["causal_link_id"],
                        pred["target_description"],
                        pred["probability"],
                        pred["rationale"],
                        pred["target_date_max"],
                    )
                    if success:
                        self.stats["predictions_written"] += 1
            else:
                logger.info(
                    "Dry run - %d predictions would be written", len(predictions)
                )
                self.stats["predictions_written"] = len(predictions)
            self.timing["write_predictions"] = time.perf_counter() - write_start

            total_elapsed = time.perf_counter() - total_start
            self.timing["total"] = total_elapsed

            # Summary
            logger.info("")
            logger.info("-" * 60)
            logger.info("RUN SUMMARY")
            logger.info("-" * 60)
            logger.info("Events loaded: %d", self.stats["events_loaded"])
            logger.info("Causal links loaded: %d", self.stats["causal_links_loaded"])
            logger.info(
                "Predictions generated: %d", self.stats["predictions_generated"]
            )
            logger.info("Predictions written: %d", self.stats["predictions_written"])
            logger.info(
                "Predictions skipped (dupe): %d", self.stats["predictions_skipped_dupe"]
            )
            logger.info("Predictions failed: %d", self.stats["predictions_failed"])
            logger.info("Total elapsed time: %.3fs", total_elapsed)
            logger.info("-" * 60)

            # RUN_VALIDATION block for machine parsing
            validation = {
                "run_id": datetime.now(timezone.utc).isoformat(),
                "mode": "dry_run" if DRY_RUN else "production",
                "success": True,
                "timing_breakdown": self.timing,
                "stats": self.stats,
                "db_queries": len(_instrumentation["db_queries"]),
                "api_calls": len(_instrumentation["api_calls"]),
            }

            print("\nRUN_VALIDATION_START")
            print(json.dumps(validation, indent=2, default=str))
            print("RUN_VALIDATION_END")

            return {
                "run_id": datetime.now(timezone.utc).isoformat(),
                "mode": "dry_run" if DRY_RUN else "production",
                "stats": self.stats,
                "predictions": predictions[:5],
                "elapsed_seconds": total_elapsed,
                "timing_breakdown": self.timing,
            }
        finally:
            await self.close()


async def main():
    """Entry point."""
    logger.info("Starting Volva Predictor main()")
    predictor = VolvaPredictor()

    try:
        result = await predictor.run()

        logger.debug("Machine output follows")
        print("\nMACHINE_OUTPUT_START")
        print(json.dumps(result, indent=2, default=str))
        print("MACHINE_OUTPUT_END")

    except Exception as e:
        logger.critical("Critical Error in main(): %s", e)
        import traceback

        logger.critical("Traceback: %s", traceback.format_exc())
        print("\n[!!!] Critical Error: {0}".format(e))
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
