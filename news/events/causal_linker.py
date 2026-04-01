#!/usr/bin/env python3
"""
Volva Causal Link Discovery - Phase 1 (REVISED)
Fetches events from SurrealDB, uses Gemini 2.0/2.5 to identify causal links
within sliding 24-hour windows, and inserts them into the causal_link table.

Usage:
    DRY_RUN=false MAX_CANDIDATE_PAIRS=100 python3 causal_linker.py

Changes in this version:
    - Support environment variable overrides for DRY_RUN and MAX_CANDIDATE_PAIRS
    - Strictly enforce from.timestamp < to.timestamp
    - Improved logging for connection and discovery progress
    - Better error handling for SurrealDB and Gemini API
    - Graceful shutdown handling (SIGTERM/SIGINT)
    - Checkpoint/resume capability for long-running runs
    - Intermediate checkpoint saving every 10 pairs
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from surrealdb import AsyncSurreal

# Add parent directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from causal_linker_config import (
    SURREALDB_HOST,
    SURREALDB_USER,
    SURREALDB_PASS,
    SURREALDB_NS,
    SURREALDB_DB,
    GEMINI_API_KEY_ENV,
    GEMINI_MODEL,
    GEMINI_API_BASE,
    WINDOW_HOURS,
    CONFIDENCE_THRESHOLD,
    MAX_CANDIDATE_PAIRS as CONFIG_MAX_CANDIDATE_PAIRS,
    BATCH_SIZE,
    MAX_RETRIES,
    RETRY_BACKOFF,
    DRY_RUN as CONFIG_DRY_RUN,
    VERBOSE,
    CATEGORY_INDICATOR_MAP,
    CATEGORY_KEYWORDS,
)

# Environment variable overrides
DRY_RUN = os.environ.get("DRY_RUN", str(CONFIG_DRY_RUN)).lower() == "true"
MAX_CANDIDATE_PAIRS = int(
    os.environ.get("MAX_CANDIDATE_PAIRS", CONFIG_MAX_CANDIDATE_PAIRS)
)


class CausalLinker:
    """Causal link discovery using Gemini 2.0/2.5 and SurrealDB."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.gemini_api_key: Optional[str] = None
        self.stats = {
            "events_loaded": 0,
            "indicators_loaded": 0,
            "pairs_generated": 0,
            "pairs_scored": 0,
            "pairs_failed": 0,
            "links_accepted": 0,
            "links_rejected": 0,
            "links_duplicated": 0,
            "db_writes": 0,
            "errors": [],
        }
        self.accepted_links: list = []
        self.rejected_pairs: list = []
        self.failed_pairs: list = []
        self.seen_pair_keys: set = set()  # For deduplication within run
        self.shutdown_requested: bool = False  # Graceful shutdown flag
        self.checkpoint_file: Optional[str] = None  # Path to checkpoint file

    def request_shutdown(self):
        """Request graceful shutdown. Called by signal handler."""
        print("\n[!] Shutdown requested, finishing current operation...")
        self.shutdown_requested = True

    def load_checkpoint(self) -> dict:
        """Load checkpoint if exists. Returns empty dict if no checkpoint."""
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            return {}
        try:
            with open(self.checkpoint_file, "r") as f:
                data = json.load(f)
            print(
                f"[*] Loaded checkpoint: {len(data.get('processed_pairs', []))} pairs already processed"
            )
            return data
        except Exception as e:
            print(f"[!] Failed to load checkpoint: {e}")
            return {}

    def save_checkpoint(self, pairs_processed: list, results_so_far: list):
        """Save checkpoint of processed pairs and results."""
        if not self.checkpoint_file:
            return
        try:
            checkpoint = {
                "pairs_processed": pairs_processed,
                "results_so_far": results_so_far,
                "stats": self.stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            with open(self.checkpoint_file, "w") as f:
                json.dump(checkpoint, f, indent=2, default=str)
            print(f"[*] Checkpoint saved: {len(pairs_processed)} pairs processed")
        except Exception as e:
            print(f"[!] Failed to save checkpoint: {e}")

    async def connect(self):
        """Connect to SurrealDB with logging."""
        print(f"[*] Connecting to SurrealDB at {SURREALDB_HOST}...")
        try:
            self.db = AsyncSurreal(SURREALDB_HOST)
            await self.db.signin(
                {"username": SURREALDB_USER, "password": SURREALDB_PASS}
            )
            await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)
            print(f"[+] Connected successfully to {SURREALDB_NS}/{SURREALDB_DB}")
        except Exception as e:
            print(f"[!] Failed to connect to SurrealDB: {e}")
            raise

    async def close(self):
        """Close SurrealDB connection."""
        if self.db:
            try:
                print("[*] Closing SurrealDB connection...")
                await self.db.close()
            except Exception as e:
                print(f"[!] Error closing connection: {e}")

    def get_gemini_api_key(self) -> str:
        """Get Gemini API key from environment."""
        key = os.environ.get(GEMINI_API_KEY_ENV)
        if not key:
            raise ValueError(f"{GEMINI_API_KEY_ENV} environment variable not set")
        return key

    async def load_events(self) -> list[dict]:
        """Load all events from SurrealDB, sorted by timestamp."""
        print("[*] Loading events from SurrealDB...")
        query = """
            SELECT * FROM event 
            ORDER BY timestamp ASC;
        """
        try:
            result = await self.db.query(query)
            # handle both new and old surrealdb-python result formats
            events = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    events = result[0]["result"]
                else:
                    events = result

            # Filter out events without proper content
            events = [e for e in events if e.get("content")]

            self.stats["events_loaded"] = len(events)
            print(f"[+] Loaded {len(events)} events from SurrealDB")
            return events
        except Exception as e:
            print(f"[!] Error loading events: {e}")
            raise

    async def load_indicators(self) -> list[dict]:
        """Load all indicator_series from SurrealDB."""
        print("[*] Loading indicators from SurrealDB...")
        query = "SELECT * FROM indicator_series;"
        try:
            result = await self.db.query(query)
            indicators = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    indicators = result[0]["result"]
                else:
                    indicators = result

            indicators = [i for i in indicators if i.get("name")]

            self.stats["indicators_loaded"] = len(indicators)
            print(f"[+] Loaded {len(indicators)} indicators from SurrealDB")
            return indicators
        except Exception as e:
            print(f"[!] Error loading indicators: {e}")
            return []

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

        print(f"    [>] Detected categories: {matched_categories}")
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

        print(
            f"    [>] Loading indicators for categories {categories} (series: {series_ids})"
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

            print(
                f"    [>] Loaded {len(indicators)} category-specific indicator observations"
            )
            return indicators
        except Exception as e:
            print(f"    [!] Failed to load indicators for categories {categories}: {e}")
            return {}

    def parse_timestamp(self, ts) -> Optional[datetime]:
        """Robust timestamp parsing."""
        if not ts:
            return None
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                # Handle SurrealDB or ISO formats
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def score_pair_relevance(self, event_a: dict, event_b: dict) -> float:
        """
        Compute a pre-score for a candidate pair before Gemini scoring.

        This implements selectivity: higher scores indicate pairs more likely
        to have meaningful causal relationships, allowing us to prioritize
        high-signal pairs for scoring.

        Scoring factors:
        - Category overlap: +0.3 per matching category (max 0.6)
        - Temporal proximity: up to 0.3 (closer events score higher)
        - Content keyword overlap: up to 0.2 (shared significant terms)

        Returns:
            Float score in range [0.0, 1.0]
        """
        if self._is_indicator(event_b):
            # Event -> Indicator pairs: score based on category match with indicator
            event_content = event_a.get("content", "").lower()
            indicator_name = event_b.get("name", "").lower()
            indicator_desc = event_b.get("description", "").lower()

            # Check if event mentions the indicator category
            score = 0.0

            # Check category keywords for the indicator
            for category, keywords in CATEGORY_KEYWORDS.items():
                for kw in keywords:
                    if kw.lower() in event_content:
                        # Check if indicator belongs to this category
                        if category in CATEGORY_INDICATOR_MAP:
                            series_ids = CATEGORY_INDICATOR_MAP[category]
                            indicator_series_id = event_b.get("series_id", "")
                            if any(
                                sid.lower() in indicator_name
                                or sid.lower() in indicator_desc
                                for sid in series_ids
                            ):
                                score += 0.4
                                break

            # Proximity bonus for recent events
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if ts_a:
                hours_old = (datetime.now(timezone.utc) - ts_a).total_seconds() / 3600
                if hours_old < 6:
                    score += 0.3
                elif hours_old < 24:
                    score += 0.15

            return min(1.0, score)

        # Event -> Event pairs
        content_a = event_a.get("content", "").lower()
        content_b = event_b.get("content", "").lower()

        score = 0.0

        # 1. Category overlap (max 0.6)
        categories_a = self.detect_news_categories(event_a.get("content", ""))
        categories_b = self.detect_news_categories(event_b.get("content", ""))
        shared_categories = set(categories_a) & set(categories_b)
        score += min(0.6, len(shared_categories) * 0.3)

        # 2. Temporal proximity (max 0.3)
        ts_a = self.parse_timestamp(event_a.get("timestamp"))
        ts_b = self.parse_timestamp(event_b.get("timestamp"))
        if ts_a and ts_b:
            hours_diff = abs((ts_b - ts_a).total_seconds() / 3600)
            if hours_diff < 1:
                score += 0.3
            elif hours_diff < 6:
                score += 0.2
            elif hours_diff < 12:
                score += 0.1

        # 3. Content keyword overlap (max 0.2)
        # Extract significant words (len > 4) from both contents
        words_a = set(w for w in content_a.split() if len(w) > 4)
        words_b = set(w for w in content_b.split() if len(w) > 4)
        shared_words = words_a & words_b
        if shared_words:
            overlap_ratio = (
                len(shared_words) / min(len(words_a), len(words_b))
                if min(len(words_a), len(words_b)) > 0
                else 0
            )
            score += min(0.2, overlap_ratio * 0.2)

        return min(1.0, score)

    def build_candidate_pairs(
        self, events: list[dict], indicators: list[dict]
    ) -> list[tuple[dict, dict, float]]:
        """Build candidate pairs within sliding 24-hour windows.

        Generates two types of pairs:
        - event -> event: within 24h sliding windows (temporal precedence enforced)
        - event -> indicator: events within 24h paired with all indicators (potential influence)

        Pairs are returned as tuples (event_a, event_b, relevance_score) sorted
        by relevance score descending for selectivity (high-signal pairs first).

        Strictly enforce for event->event: A.timestamp < B.timestamp
        """
        print("[*] Generating candidate pairs within sliding windows...")
        pairs_with_scores = []
        n = len(events)

        # Event -> Event pairs
        event_event_count = 0
        for i, event_a in enumerate(events):
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if not ts_a:
                continue

            window_end = ts_a + timedelta(hours=WINDOW_HOURS)

            # Look for events B within the window
            for j in range(i + 1, n):
                event_b = events[j]
                ts_b = self.parse_timestamp(event_b.get("timestamp"))

                if not ts_b:
                    continue

                # Strict temporal precedence: A must be before B
                if ts_b <= ts_a:
                    continue

                # Check if within window
                if ts_b <= window_end:
                    relevance = self.score_pair_relevance(event_a, event_b)
                    pairs_with_scores.append((event_a, event_b, relevance))
                    event_event_count += 1
                else:
                    # Events are sorted, so no need to check further for this A
                    break

        # Event -> Indicator pairs (event drives indicator change)
        event_indicator_count = 0
        if indicators:
            print(f"[*] Generating event -> indicator pairs...")
            recent_events = [
                e for e in events if self.parse_timestamp(e.get("timestamp"))
            ]
            for event_a in recent_events:
                ts_a = self.parse_timestamp(event_a.get("timestamp"))
                if not ts_a:
                    continue
                # Only include events within WINDOW_HOURS from now
                if datetime.now(timezone.utc) - ts_a > timedelta(hours=WINDOW_HOURS):
                    continue
                for indicator in indicators:
                    relevance = self.score_pair_relevance(event_a, indicator)
                    pairs_with_scores.append((event_a, indicator, relevance))
                    event_indicator_count += 1

        # Sort by relevance score descending (selectivity: high-signal pairs first)
        pairs_with_scores.sort(key=lambda x: x[2], reverse=True)

        # Apply safety limit - but keep top-scoring pairs
        original_count = len(pairs_with_scores)
        if original_count > MAX_CANDIDATE_PAIRS:
            print(
                f"[!] {original_count} pairs generated, keeping top {MAX_CANDIDATE_PAIRS} by relevance"
            )
            pairs_with_scores = pairs_with_scores[:MAX_CANDIDATE_PAIRS]
        else:
            print(
                f"[+] Generated {original_count} candidate pairs (sorted by relevance)"
            )

        print(f"    - Event->Event pairs: {event_event_count}")
        print(f"    - Event->Indicator pairs: {event_indicator_count}")

        # Report selectivity metrics
        if pairs_with_scores:
            scores = [p[2] for p in pairs_with_scores]
            avg_score = sum(scores) / len(scores)
            top_score = max(scores)
            print(f"    - Relevance scores: avg={avg_score:.3f}, max={top_score:.3f}")

        self.stats["pairs_generated"] = len(pairs_with_scores)
        # Return as tuple for compatibility
        return [(p[0], p[1]) for p in pairs_with_scores]

    def get_pair_key(self, event_a: dict, event_b: dict) -> str:
        """Generate deterministic deduplication key for a pair."""
        id_a = str(event_a.get("id", ""))
        id_b = str(event_b.get("id", ""))
        # For causal links, order matters, but here we just need to know if we checked this specific direction
        return f"{id_a}->{id_b}"

    def _is_indicator(self, target: dict) -> bool:
        """Check if target is an indicator_series record."""
        # Indicators have 'name' and 'series_id' which events don't have
        return bool(target.get("name") and target.get("series_id"))

    async def score_pair_with_gemini(
        self,
        event_a: dict,
        event_b: dict,
        client: httpx.AsyncClient,
    ) -> dict:
        """Query Gemini to determine if event A causes event B or influences an indicator."""

        # Check if target is an indicator
        if self._is_indicator(event_b):
            return await self._score_event_indicator_pair(event_a, event_b, client)

        # Phase 6: Detect news categories and load relevant indicators for event->event pairs
        event_a_content = event_a.get("content", "")
        detected_categories = self.detect_news_categories(event_a_content)
        category_indicators = {}
        if detected_categories:
            category_indicators = await self.load_indicators_for_categories(
                detected_categories
            )

        # Build category indicator context for prompt
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

        # Standard event -> event scoring
        prompt = f"""You are a causal analysis expert. Given two events, determine if the first event plausibly caused the second event.
Return ONLY valid JSON: {{"causal": true/false, "confidence": 0.0-1.0, "mechanism": "brief explanation"}}

Event A: {event_a_content[:1000]}
Event A timestamp: {event_a.get("timestamp")}
{category_indicator_context}Event B: {event_b.get("content", "")[:1000]}
Event B timestamp: {event_b.get("timestamp")}

Analysis: Did Event A plausibly cause Event B? Consider:
- Temporal precedence (A must come before B)
- Mechanism plausibility (is there a logical link?)
- Context (are they related to the same geopolitical or technical domain?){"[ Also consider the related economic indicators above. ]" if category_indicator_context else ""}

Response:"""

        url = f"{GEMINI_API_BASE}/{GEMINI_MODEL}:generateContent"
        params = {"key": self.gemini_api_key}
        headers = {"Content-Type": "application/json"}
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 1024,
                "temperature": 0.3,
            },
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = await client.post(
                    url, json=body, headers=headers, params=params, timeout=45.0
                )

                if response.status_code == 429:
                    wait_time = RETRY_BACKOFF * (2**attempt)
                    print(f"    [!] Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue

                if response.status_code != 200:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                    }

                data = response.json()

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
                    json_start = text.find("{")
                    json_end = text.rfind("}") + 1
                    if json_start >= 0 and json_end > json_start:
                        json_str = text[json_start:json_end]
                        result = json.loads(json_str)
                    else:
                        result = json.loads(text.strip())

                    return {
                        "success": True,
                        "causal": result.get("causal", False),
                        "confidence": float(result.get("confidence", 0.0)),
                        "mechanism": str(result.get("mechanism", "")),
                    }
                except json.JSONDecodeError as e:
                    return {
                        "success": False,
                        "error": f"JSON parse error: {e}",
                        "raw": text[:200],
                    }

            except (asyncio.TimeoutError, httpx.TimeoutException):
                if attempt < MAX_RETRIES - 1:
                    print(f"    [!] Timeout on attempt {attempt + 1}, retrying...")
                    continue
                return {"success": False, "error": "Request timeout after retries"}
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
                    continue
                return {"success": False, "error": str(e)}

        return {"success": False, "error": "Max retries exceeded"}

    async def _score_event_indicator_pair(
        self,
        event: dict,
        indicator: dict,
        client: httpx.AsyncClient,
    ) -> dict:
        """Score an event -> indicator pair with specialized prompt."""
        prompt = f"""You are a causal analysis expert specializing in economic indicators. Given a news event and an economic indicator, determine if the event plausibly influenced or caused a significant change in that indicator.
Return ONLY valid JSON: {{"causal": true/false, "confidence": 0.0-1.0, "mechanism": "brief explanation"}}

News Event: {event.get("content", "")[:1000]}
Event timestamp: {event.get("timestamp")}

Economic Indicator: {indicator.get("name", "Unknown")}
Description: {indicator.get("description", "No description available")}
Unit: {indicator.get("unit", "N/A")}
Series ID: {indicator.get("series_id", "N/A")}
Source: {indicator.get("source", "Unknown")}

Analysis: Could this news event have meaningfully influenced or predicted a change in this economic indicator? Consider:
- Economic mechanism plausibility (does the event relate to factors that affect this indicator?)
- Relevance to the indicator's underlying drivers (e.g., Fed funds rate for interest-related indicators)
- Historical correlation plausibility
- The unit scale: {indicator.get("unit", "N/A")} - would this event be significant enough to move this metric?

Response:"""

        url = f"{GEMINI_API_BASE}/{GEMINI_MODEL}:generateContent"
        params = {"key": self.gemini_api_key}
        headers = {"Content-Type": "application/json"}
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 1024,
                "temperature": 0.3,
            },
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = await client.post(
                    url, json=body, headers=headers, params=params, timeout=45.0
                )

                if response.status_code == 429:
                    wait_time = RETRY_BACKOFF * (2**attempt)
                    print(f"    [!] Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue

                if response.status_code != 200:
                    return {
                        "success": False,
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                    }

                data = response.json()

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
                    json_start = text.find("{")
                    json_end = text.rfind("}") + 1
                    if json_start >= 0 and json_end > json_start:
                        json_str = text[json_start:json_end]
                        result = json.loads(json_str)
                    else:
                        result = json.loads(text.strip())

                    return {
                        "success": True,
                        "causal": result.get("causal", False),
                        "confidence": float(result.get("confidence", 0.0)),
                        "mechanism": str(result.get("mechanism", "")),
                    }
                except json.JSONDecodeError as e:
                    return {
                        "success": False,
                        "error": f"JSON parse error: {e}",
                        "raw": text[:200],
                    }

            except (asyncio.TimeoutError, httpx.TimeoutException):
                if attempt < MAX_RETRIES - 1:
                    print(f"    [!] Timeout on attempt {attempt + 1}, retrying...")
                    continue
                return {"success": False, "error": "Request timeout after retries"}
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
                    continue
                return {"success": False, "error": str(e)}

        return {"success": False, "error": "Max retries exceeded"}

    async def score_batch(
        self,
        pairs: list[tuple[dict, dict]],
    ) -> list[dict]:
        """Score a batch of pairs with Gemini. Supports graceful shutdown and checkpointing."""
        if not pairs:
            return []

        # Load checkpoint if exists
        checkpoint_data = self.load_checkpoint()
        processed_pairs = checkpoint_data.get("pairs_processed", [])
        results = checkpoint_data.get("results_so_far", [])

        # Build set of already processed pair keys for quick lookup
        processed_keys = set()
        for pair in processed_pairs:
            key = self.get_pair_key(pair[0], pair[1])
            processed_keys.add(key)

        # Skip already processed pairs
        remaining_pairs = [
            p for p in pairs if self.get_pair_key(p[0], p[1]) not in processed_keys
        ]
        start_idx = len(processed_pairs)

        if remaining_pairs:
            print(
                f"[*] Scoring {len(remaining_pairs)} remaining pairs (skipping {start_idx} already processed)"
            )

        self.gemini_api_key = self.get_gemini_api_key()

        async with httpx.AsyncClient() as client:
            # Process remaining pairs
            for i, (event_a, event_b) in enumerate(remaining_pairs):
                # Check for shutdown request
                if self.shutdown_requested:
                    print(
                        f"\n[!] Shutdown requested during scoring. Saving checkpoint..."
                    )
                    self.save_checkpoint(processed_pairs + remaining_pairs[:i], results)
                    print(
                        f"[*] Checkpoint saved. Processed {len(results)} pairs so far."
                    )
                    print("[*] Run script again to resume from checkpoint.")
                    break

                pair_idx = start_idx + i + 1
                total_pairs = len(pairs)
                print(f"  [*] Scoring pair {pair_idx}/{total_pairs}...")

                # Log target info - event or indicator
                target_id = event_b.get("id")
                if self._is_indicator(event_b):
                    print(f"      A: {event_a.get('id')} ({event_a.get('timestamp')})")
                    print(f"      B: [INDICATOR] {event_b.get('name')} ({target_id})")
                else:
                    print(f"      A: {event_a.get('id')} ({event_a.get('timestamp')})")
                    print(f"      B: {event_b.get('id')} ({event_b.get('timestamp')})")

                result = await self.score_pair_with_gemini(event_a, event_b, client)
                result["from_event"] = event_a
                result["to_event"] = event_b
                results.append(result)
                processed_pairs.append((event_a, event_b))

                if result.get("success"):
                    is_causal = result.get("causal", False)
                    conf = result.get("confidence", 0.0)
                    print(f"      [?] Causal: {is_causal}, Conf: {conf:.2f}")
                else:
                    print(f"      [!] Failed: {result.get('error')}")

                # Save checkpoint every 10 pairs
                if (i + 1) % 10 == 0:
                    self.save_checkpoint(processed_pairs, results)
                    print(f"[*] Intermediate checkpoint saved at pair {pair_idx}")

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)

        return results

    async def check_link_exists(self, from_id: str, to_id: str) -> bool:
        """Check if causal link already exists in database."""
        query = (
            "SELECT id FROM causal_link WHERE from = $from_id AND to = $to_id LIMIT 1;"
        )
        try:
            result = await self.db.query(query, {"from_id": from_id, "to_id": to_id})
            # handle both result formats
            if isinstance(result, list) and result:
                if isinstance(result[0], dict) and "result" in result[0]:
                    return len(result[0]["result"]) > 0
                return len(result) > 0
            return False
        except Exception:
            return False

    async def write_causal_link(
        self,
        from_id: str,
        to_id: str,
        confidence: float,
        mechanism: str,
    ) -> bool:
        """Write a causal link to SurrealDB."""
        run_time = datetime.now(timezone.utc).isoformat()

        # Format strings for SQL
        clean_mechanism = mechanism.replace('"', '\\"').replace("\n", " ")

        query = f"""
            CREATE causal_link SET
            from = {from_id},
            to = {to_id},
            confidence = {confidence},
            mechanism = "{clean_mechanism}",
            evidence = [],
            provenance = {{
                "model": "{GEMINI_MODEL}",
                "run_time": "{run_time}",
                "window_hours": {WINDOW_HOURS}
            }},
            correct_predictions = 0,
            total_predictions = 0,
            feedback_history = [];
        """

        try:
            await self.db.query(query)
            return True
        except Exception as e:
            print(f"      [!] DB write error: {e}")
            self.stats["errors"].append(
                {
                    "type": "db_write",
                    "from": from_id,
                    "to": to_id,
                    "error": str(e),
                }
            )
            return False

    async def process_pairs(self, pairs: list[tuple[dict, dict]]) -> list[dict]:
        """Process all candidate pairs and return accepted links."""
        print(f"[*] Processing {len(pairs)} candidate pairs...")

        all_results = await self.score_batch(pairs)

        # Process results
        accepted = []
        for result in all_results:
            self.stats["pairs_scored"] += 1

            if not result.get("success"):
                self.stats["pairs_failed"] += 1
                self.failed_pairs.append(
                    {
                        "from": result["from_event"].get("id"),
                        "to": result["to_event"].get("id"),
                        "error": result.get("error"),
                    }
                )
                continue

            event_a = result["from_event"]
            event_b = result["to_event"]
            from_id = event_a.get("id")
            to_id = event_b.get("id")

            # Apply confidence threshold
            causal = result.get("causal", False)
            confidence = result.get("confidence", 0.0)
            mechanism = result.get("mechanism", "")

            if not causal or confidence < CONFIDENCE_THRESHOLD:
                self.stats["links_rejected"] += 1
                self.rejected_pairs.append(
                    {
                        "from": from_id,
                        "to": to_id,
                        "confidence": confidence,
                        "reason": "below_threshold" if not causal else "low_confidence",
                    }
                )
                continue

            # Check for duplicates (already processed in this run)
            pair_key = self.get_pair_key(event_a, event_b)
            if pair_key in self.seen_pair_keys:
                self.stats["links_duplicated"] += 1
                continue
            self.seen_pair_keys.add(pair_key)

            # Final check against DB
            if not DRY_RUN:
                exists = await self.check_link_exists(from_id, to_id)
                if exists:
                    print(f"      [-] Link already exists in DB: {from_id} -> {to_id}")
                    self.stats["links_duplicated"] += 1
                    continue

                # Write to database
                success = await self.write_causal_link(
                    from_id, to_id, confidence, mechanism
                )
                if success:
                    print(f"      [+] Link accepted and written: {from_id} -> {to_id}")
                    self.stats["links_accepted"] += 1
                    self.stats["db_writes"] += 1
                    accepted.append(
                        {
                            "from": from_id,
                            "to": to_id,
                            "confidence": confidence,
                            "mechanism": mechanism,
                        }
                    )
            else:
                print(f"      [+] Link accepted (Dry Run): {from_id} -> {to_id}")
                self.stats["links_accepted"] += 1
                accepted.append(
                    {
                        "from": from_id,
                        "to": to_id,
                        "confidence": confidence,
                        "mechanism": mechanism,
                    }
                )

        return accepted

    async def run(self) -> dict:
        """Execute the full causal link discovery pipeline."""
        print("=" * 60)
        print("Volva Causal Link Discovery - Phase 1 (REVISED)")
        print("=" * 60)
        print(f"Mode: {'DRY_RUN' if DRY_RUN else 'PRODUCTION'}")
        print(f"Max candidate pairs: {MAX_CANDIDATE_PAIRS}")

        await self.connect()

        try:
            events = await self.load_events()
            if not events:
                print("[!] No events found in database")
                return {"stats": self.stats, "accepted_links": []}

            indicators = await self.load_indicators()
            pairs = self.build_candidate_pairs(events, indicators)
            if not pairs:
                print("[!] No candidate pairs generated")
                return {"stats": self.stats, "accepted_links": []}

            start_time = time.time()
            accepted = await self.process_pairs(pairs)
            elapsed = time.time() - start_time

            # Summary
            print()
            print("-" * 60)
            print("RUN SUMMARY")
            print("-" * 60)
            print(f"Events loaded: {self.stats['events_loaded']}")
            print(f"Indicators loaded: {self.stats['indicators_loaded']}")
            print(f"Pairs generated: {self.stats['pairs_generated']}")
            print(f"Pairs scored: {self.stats['pairs_scored']}")
            print(f"Pairs failed: {self.stats['pairs_failed']}")
            print(f"Links accepted: {self.stats['links_accepted']}")
            print(f"Links rejected: {self.stats['links_rejected']}")
            print(f"Links duplicated: {self.stats['links_duplicated']}")
            print(f"DB writes: {self.stats['db_writes']}")
            print(f"Elapsed time: {elapsed:.1f}s")
            print("-" * 60)

            return {
                "run_id": datetime.now(timezone.utc).isoformat(),
                "mode": "dry_run" if DRY_RUN else "production",
                "stats": self.stats,
                "accepted_links": accepted,
                "elapsed_seconds": elapsed,
            }
        finally:
            await self.close()


async def main():
    """Entry point with graceful shutdown support."""
    linker = CausalLinker()

    # Set checkpoint file path
    checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/tmp")
    linker.checkpoint_file = os.path.join(
        checkpoint_dir, f"causal_linker_checkpoint_{os.getpid()}.json"
    )

    # Set up signal handlers for graceful shutdown
    def shutdown_handler(signum, frame):
        print(f"\n[!] Received signal {signum}, initiating graceful shutdown...")
        linker.request_shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        result = await linker.run()

        # Clean up checkpoint file on successful completion
        if linker.checkpoint_file and os.path.exists(linker.checkpoint_file):
            try:
                os.remove(linker.checkpoint_file)
                print(f"[*] Checkpoint file cleaned up")
            except Exception:
                pass

        print("\nMACHINE_OUTPUT_START")
        print(json.dumps(result, indent=2, default=str))
        print("MACHINE_OUTPUT_END")

    except Exception as e:
        print(f"\n[!!!] Critical Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
