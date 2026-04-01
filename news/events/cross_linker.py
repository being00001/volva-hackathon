#!/usr/bin/env python3
"""
Volva Cross-Link Discovery - Phase 6 (NER + Thematic Embeddings)
Discovers causal links across extended temporal windows (days/weeks) using:
- spaCy NER for entity extraction (people/orgs/locations)
- Gemini embeddings for thematic similarity (cosine>0.8)
- Entity similarity filtering (Jaccard>0.3)
- Known lagged patterns (economic/geopolitical)
- Multi-window expansion

Usage:
    DRY_RUN=true MAX_CANDIDATE_PAIRS=10 python3 cross_linker.py

Requires:
    - GOOGLE_AIS_API_KEY environment variable for Gemini API
    - SurrealDB running at http://127.0.0.1:8001
    - spaCy model: en_core_web_sm

Changes in this version:
    - Graceful shutdown handling (SIGTERM/SIGINT)
    - Checkpoint/resume capability for long-running runs
    - Intermediate checkpoint saving every 50 pairs
"""

import asyncio
import hashlib
import json
import os
import random
import re
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from surrealdb import AsyncSurreal

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cross_linker_config import (
    # Base settings
    SURREALDB_HOST,
    SURREALDB_USER,
    SURREALDB_PASS,
    SURREALDB_NS,
    SURREALDB_DB,
    GEMINI_API_KEY_ENV,
    GEMINI_MODEL,
    GEMINI_API_BASE,
    MAX_RETRIES,
    RETRY_BACKOFF,
    BATCH_SIZE,
    # Cross-link settings
    CROSS_LINK_ENABLED,
    CROSS_LINK_WINDOW_DAYS,
    CROSS_LINK_MAX_PAIRS,
    CONFIDENCE_THRESHOLD,
    # Window tiers
    WINDOW_TIERS,
    # Entity tracking
    ENTITY_TRACKING_ENABLED,
    ENTITY_WINDOW_DAYS,
    MIN_ENTITY_OCCURRENCES,
    ENTITY_CONFIDENCE_BOOST,
    ENTITY_PATTERNS,
    # Thematic tracking
    THEMATIC_TRACKING_ENABLED,
    THEMATIC_WINDOW_DAYS,
    MIN_TOPIC_OVERLAP,
    THEMATIC_KEYWORDS,
    # Lagged patterns
    LAG_PATTERNS_ENABLED,
    LAG_PATTERNS,
    LAG_CONFIDENCE_BOOST,
    # Hierarchical patterns
    HIERARCHICAL_PATTERNS,
    # Phase 6: NER and Embeddings
    NER_ENABLED,
    NER_MODEL,
    NER_ENTITY_TYPES,
    ENTITY_SIMILARITY_THRESHOLD,
    USE_GEMINI_EMBEDDINGS,
    GEMINI_EMBEDDING_MODEL,
    GEMINI_EMBEDDING_THRESHOLD,
    ENTITY_THEMATIC_BOOST,
    THEMATIC_FALLBACK_ENABLED,
    # Run settings
    DRY_RUN as CONFIG_DRY_RUN,
    VERBOSE,
)

# Import spaCy for NER (lazy loading to avoid startup overhead if disabled)
spacy = None
nlp_model = None


def get_nlp_model():
    """Lazy load spaCy model only when NER is enabled."""
    global spacy, nlp_model
    if nlp_model is None and NER_ENABLED:
        try:
            import spacy as spacy_lib

            spacy = spacy_lib
            nlp_model = spacy.load(NER_MODEL)
        except Exception as e:
            print(f"[!] Failed to load spaCy model {NER_MODEL}: {e}")
            print("[!] Falling back to regex-based entity extraction")
    return nlp_model


# Environment variable overrides
DRY_RUN = os.environ.get("DRY_RUN", str(CONFIG_DRY_RUN)).lower() == "true"
MAX_CANDIDATE_PAIRS = int(os.environ.get("MAX_CANDIDATE_PAIRS", CROSS_LINK_MAX_PAIRS))


class CrossLinker:
    """Cross-link discovery using extended temporal windows and multi-strategy matching."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.gemini_api_key: Optional[str] = None
        self.stats = {
            "events_loaded": 0,
            "pairs_entity": 0,
            "pairs_thematic": 0,
            "pairs_lag": 0,
            "pairs_multi_window": 0,
            "pairs_hierarchical": 0,
            "pairs_total": 0,
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
            except (AttributeError, NotImplementedError) as e:
                # Some connection types don't implement close
                print(f"[*] Connection cleanup skipped: {e}")
            except Exception as e:
                print(f"[!] Error closing connection: {e}")

    def get_gemini_api_key(self) -> str:
        """Get Gemini API key from environment."""
        key = os.environ.get(GEMINI_API_KEY_ENV)
        if not key:
            raise ValueError(f"{GEMINI_API_KEY_ENV} environment variable not set")
        return key

    async def load_events(self, days: int = 30) -> list[dict]:
        """Load events from SurrealDB within specified time window."""
        print(f"[*] Loading events from SurrealDB (last {days} days)...")

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        query = f"""
            SELECT * FROM event 
            WHERE timestamp >= '{cutoff.isoformat()}'
            ORDER BY timestamp ASC;
        """
        try:
            result = await self.db.query(query)
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

    # =========================================================================
    # ENTITY TRACKING (Phase 6: spaCy NER)
    # =========================================================================

    def extract_entities(self, event_content: str) -> set[str]:
        """Extract named entities from event content using spaCy NER or regex fallback."""
        entities = set()

        # Try spaCy NER first if enabled
        if NER_ENABLED:
            nlp = get_nlp_model()
            if nlp is not None:
                try:
                    doc = nlp(event_content)
                    for ent in doc.ents:
                        if ent.label_ in NER_ENTITY_TYPES:
                            # Normalize entity text
                            entity_text = ent.text.strip().lower()
                            if entity_text:
                                entities.add(entity_text)
                    return entities
                except Exception as e:
                    if VERBOSE:
                        print(f"      [!] spaCy NER failed: {e}")

        # Fallback to regex-based extraction
        content_lower = event_content.lower()

        for pattern in ENTITY_PATTERNS:
            matches = re.findall(pattern, event_content, re.IGNORECASE)
            for match in matches:
                entities.add(match.lower())

        # Also add specific keyword-based entities
        # Countries/regions
        geo_terms = [
            "ukraine",
            "russia",
            "china",
            "usa",
            "iran",
            "israel",
            "saudi arabia",
            "korea",
            "north korea",
            "south korea",
            "japan",
            "germany",
            "france",
            "uk",
            "britain",
            "india",
            "brazil",
            "argentina",
            "turkey",
        ]
        for term in geo_terms:
            if term in content_lower:
                entities.add(term)

        return entities

    def calculate_entity_jaccard(
        self, entities_a: set[str], entities_b: set[str]
    ) -> float:
        """Calculate Jaccard similarity between two entity sets.

        Jaccard = |A ∩ B| / |A ∪ B|
        Returns 0.0 if either set is empty.
        """
        if not entities_a or not entities_b:
            return 0.0

        intersection = len(entities_a & entities_b)
        union = len(entities_a | entities_b)

        return intersection / union if union > 0 else 0.0

    def extract_entities_with_types(self, event_content: str) -> dict[str, set[str]]:
        """Extract entities with their types (PERSON, ORG, GPE, LOC).

        Returns dict mapping entity type to set of entity texts.
        """
        entities_by_type = {etype: set() for etype in NER_ENTITY_TYPES}

        if NER_ENABLED:
            nlp = get_nlp_model()
            if nlp is not None:
                try:
                    doc = nlp(event_content)
                    for ent in doc.ents:
                        if ent.label_ in NER_ENTITY_TYPES:
                            entity_text = ent.text.strip().lower()
                            if entity_text:
                                entities_by_type[ent.label_].add(entity_text)
                except Exception:
                    pass

        return entities_by_type

    def build_entity_index(self, events: list[dict]) -> dict[str, list[dict]]:
        """Map entity -> list of events containing that entity."""
        entity_index = defaultdict(list)

        for event in events:
            entities = self.extract_entities(event.get("content", ""))
            for entity in entities:
                entity_index[entity].append(event)

        # Filter to entities appearing in at least MIN_ENTITY_OCCURRENCES events
        filtered_index = {
            entity: events_list
            for entity, events_list in entity_index.items()
            if len(events_list) >= MIN_ENTITY_OCCURRENCES
        }

        return filtered_index

    def build_entity_cross_links(
        self, events: list[dict], entity_index: dict[str, list[dict]]
    ) -> list[tuple[dict, dict, str, float]]:
        """Generate candidate pairs from shared entities across extended windows."""
        cross_links = []

        for entity, entity_events in entity_index.items():
            # Sort by timestamp
            sorted_events = sorted(
                entity_events,
                key=lambda e: self.parse_timestamp(e.get("timestamp")) or datetime.min,
            )

            for i in range(len(sorted_events)):
                for j in range(i + 1, len(sorted_events)):
                    event_a = sorted_events[i]
                    event_b = sorted_events[j]

                    ts_a = self.parse_timestamp(event_a.get("timestamp"))
                    ts_b = self.parse_timestamp(event_b.get("timestamp"))

                    if not ts_a or not ts_b:
                        continue

                    # Skip if B is before A
                    if ts_b <= ts_a:
                        continue

                    # Check within entity window
                    days_between = (ts_b - ts_a).total_seconds() / 86400
                    if days_between <= ENTITY_WINDOW_DAYS:
                        cross_links.append(
                            (
                                event_a,
                                event_b,
                                f"shared entity: {entity}",
                                ENTITY_CONFIDENCE_BOOST,
                            )
                        )

        return cross_links

    # =========================================================================
    # GEMINI EMBEDDINGS (Phase 6)
    # =========================================================================

    async def compute_embedding(
        self,
        text: str,
        client: httpx.AsyncClient,
    ) -> Optional[list[float]]:
        """Compute Gemini embedding for text content.

        Returns embedding vector or None if failed.
        """
        if not USE_GEMINI_EMBEDDINGS:
            return None

        url = f"{GEMINI_API_BASE}/{GEMINI_EMBEDDING_MODEL}:embedContent"
        params = {"key": self.gemini_api_key}
        headers = {"Content-Type": "application/json"}

        # Truncate text if too long (embedding models have limits)
        max_chars = 8000
        truncated_text = text[:max_chars] if len(text) > max_chars else text

        body = {
            "content": {"parts": [{"text": truncated_text}]},
            "taskType": "SEMANTIC_SIMILARITY",
        }

        try:
            response = await client.post(
                url, json=body, headers=headers, params=params, timeout=30.0
            )

            if response.status_code == 429:
                if VERBOSE:
                    print(f"      [!] Rate limited on embedding request")
                return None

            if response.status_code != 200:
                if VERBOSE:
                    print(f"      [!] Embedding API error: {response.status_code}")
                return None

            data = response.json()

            # Extract embedding from response
            embedding = data.get("embedding", {})
            values = embedding.get("values", [])

            return values if values else None

        except Exception as e:
            if VERBOSE:
                print(f"      [!] Embedding computation failed: {e}")
            return None

    def cosine_similarity(self, vec_a: list[float], vec_b: list[float]) -> float:
        """Calculate cosine similarity between two vectors.

        Returns value between -1 and 1 (1 = identical, 0 = orthogonal).
        """
        if not vec_a or not vec_b or len(vec_a) != len(vec_b):
            return 0.0

        dot_product = sum(a * b for a, b in zip(vec_a, vec_b))
        magnitude_a = sum(a * a for a in vec_a) ** 0.5
        magnitude_b = sum(b * b for b in vec_b) ** 0.5

        if magnitude_a == 0 or magnitude_b == 0:
            return 0.0

        return dot_product / (magnitude_a * magnitude_b)

    async def compute_thematic_similarity(
        self,
        event_a: dict,
        event_b: dict,
        client: httpx.AsyncClient,
    ) -> tuple[float, str]:
        """Compute thematic similarity using Gemini embeddings.

        Returns (cosine_similarity, description) tuple.
        """
        content_a = event_a.get("content", "")
        content_b = event_b.get("content", "")

        # Compute embeddings
        emb_a = await self.compute_embedding(content_a, client)
        emb_b = await self.compute_embedding(content_b, client)

        if emb_a is None or emb_b is None:
            # Fallback to keyword-based similarity
            if THEMATIC_FALLBACK_ENABLED:
                topics_a = self.get_event_topics(content_a)
                topics_b = self.get_event_topics(content_b)
                overlap = self.calculate_topic_overlap(topics_a, topics_b)
                return overlap * 0.5, "keyword_themes"
            return 0.0, "embedding_failed"

        # Calculate cosine similarity
        similarity = self.cosine_similarity(emb_a, emb_b)

        if similarity >= GEMINI_EMBEDDING_THRESHOLD:
            return similarity, "thematic_embedding"

        return similarity, "below_threshold"

    async def build_embedding_cache(
        self,
        events: list[dict],
        client: httpx.AsyncClient,
    ) -> dict[str, list[float]]:
        """Pre-compute embeddings for all events (batch optimization).

        Returns dict mapping event_id -> embedding vector.
        """
        cache = {}

        if not USE_GEMINI_EMBEDDINGS:
            return cache

        print(f"      [*] Computing embeddings for {len(events)} events...")

        for i, event in enumerate(events):
            if i % 10 == 0:
                print(f"      [*] Embedding progress: {i}/{len(events)}")

            event_id = str(event.get("id"))
            content = event.get("content", "")

            embedding = await self.compute_embedding(content, client)
            if embedding is not None:
                cache[event_id] = embedding

            # Small delay to avoid rate limiting
            await asyncio.sleep(0.1)

        print(f"      [+] Cached {len(cache)} embeddings")
        return cache

    # =========================================================================
    # THEMATIC CONTINUITY
    # =========================================================================

    def get_event_topics(self, event_content: str) -> set[str]:
        """Identify topics present in event content."""
        content_lower = event_content.lower()
        topics = set()

        for topic, keywords in THEMATIC_KEYWORDS.items():
            if any(kw in content_lower for kw in keywords):
                topics.add(topic)

        return topics

    def calculate_topic_overlap(self, topics_a: set, topics_b: set) -> float:
        """Calculate Jaccard similarity between topic sets."""
        if not topics_a or not topics_b:
            return 0.0
        intersection = len(topics_a & topics_b)
        union = len(topics_a | topics_b)
        return intersection / union if union > 0 else 0.0

    def build_thematic_cross_links(
        self, events: list[dict]
    ) -> list[tuple[dict, dict, str, float]]:
        """Generate candidate pairs based on thematic continuity."""
        cross_links = []

        # Pre-calculate topics for all events
        event_topics = {}
        for event in events:
            topics = self.get_event_topics(event.get("content", ""))
            if topics:
                event_topics[str(event.get("id"))] = (event, topics)

        # Compare events within thematic window
        event_list = [(eid, e, t) for eid, (e, t) in event_topics.items()]

        for i, (eid_a, event_a, topics_a) in enumerate(event_list):
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if not ts_a:
                continue

            for j in range(i + 1, len(event_list)):
                eid_b, event_b, topics_b = event_list[j]

                ts_b = self.parse_timestamp(event_b.get("timestamp"))
                if not ts_b or ts_b <= ts_a:
                    continue

                # Check within thematic window
                days_between = (ts_b - ts_a).total_seconds() / 86400
                if days_between > THEMATIC_WINDOW_DAYS:
                    continue

                # Calculate topic overlap
                overlap = self.calculate_topic_overlap(topics_a, topics_b)

                if overlap >= MIN_TOPIC_OVERLAP:
                    common_topics = topics_a & topics_b
                    topic_str = ", ".join(sorted(common_topics))
                    cross_links.append(
                        (
                            event_a,
                            event_b,
                            f"thematic continuity ({topic_str})",
                            overlap * 0.1,  # Proportional boost
                        )
                    )

        return cross_links

    # =========================================================================
    # LAGGED PATTERNS
    # =========================================================================

    def check_lagged_pattern(self, event_a: dict, event_b: dict) -> Optional[dict]:
        """Check if event pair matches known lagged pattern."""
        content_a = event_a.get("content", "").lower()
        content_b = event_b.get("content", "").lower()

        for pattern_name, pattern in LAG_PATTERNS.items():
            triggers_a = any(t in content_a for t in pattern["triggers"])
            effects_b = any(e in content_b for e in pattern["effects"])

            if triggers_a and effects_b:
                ts_a = self.parse_timestamp(event_a.get("timestamp"))
                ts_b = self.parse_timestamp(event_b.get("timestamp"))

                if not ts_a or not ts_b:
                    continue

                lag_hours = (ts_b - ts_a).total_seconds() / 3600

                if pattern["min_lag_hours"] <= lag_hours <= pattern["max_lag_hours"]:
                    return {
                        "pattern": pattern_name,
                        "lag_hours": lag_hours,
                        "confidence_boost": pattern["confidence_boost"],
                    }

        return None

    def build_lagged_cross_links(
        self, events: list[dict]
    ) -> list[tuple[dict, dict, str, float]]:
        """Generate candidate pairs from known lagged patterns."""
        cross_links = []

        for i, event_a in enumerate(events):
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if not ts_a:
                continue

            # Only check events after A
            for j in range(i + 1, len(events)):
                event_b = events[j]
                ts_b = self.parse_timestamp(event_b.get("timestamp"))

                if not ts_b or ts_b <= ts_a:
                    continue

                # Check lag pattern
                lag_match = self.check_lagged_pattern(event_a, event_b)
                if lag_match:
                    mechanism = f"lagged pattern: {lag_match['pattern']} ({lag_match['lag_hours']:.0f}h lag)"
                    cross_links.append(
                        (event_a, event_b, mechanism, lag_match["confidence_boost"])
                    )

        return cross_links

    # =========================================================================
    # MULTI-WINDOW EXPANSION
    # =========================================================================

    def build_multi_window_pairs(
        self, events: list[dict]
    ) -> list[tuple[dict, dict, str, float]]:
        """Build candidate pairs across multiple time windows."""
        all_pairs = []
        n = len(events)

        for i, event_a in enumerate(events):
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if not ts_a:
                continue

            for tier in WINDOW_TIERS:
                window_end = ts_a + timedelta(hours=tier["hours"])

                for j in range(i + 1, n):
                    event_b = events[j]
                    ts_b = self.parse_timestamp(event_b.get("timestamp"))

                    if not ts_b or ts_b <= ts_a:
                        continue

                    if ts_b <= window_end:
                        pair_key = f"{event_a.get('id')}->{event_b.get('id')}"
                        if pair_key not in all_pairs:
                            all_pairs.append(
                                (
                                    event_a,
                                    event_b,
                                    f"multi-window: {tier['name']}",
                                    tier["weight"],
                                )
                            )
                    else:
                        break  # Events sorted, no need to check further

        return all_pairs

    # =========================================================================
    # HIERARCHICAL PATTERNS
    # =========================================================================

    def check_hierarchical_pattern(self, event_a: dict, event_b: dict) -> Optional[str]:
        """Check if event pair matches hierarchical pattern."""
        content_a = event_a.get("content", "").lower()
        content_b = event_b.get("content", "").lower()

        for pattern_name, pattern in HIERARCHICAL_PATTERNS.items():
            macro_in_a = any(m in content_a for m in pattern["macro"])
            micro_in_b = any(m in content_b for m in pattern["micro"])

            if macro_in_a and micro_in_b:
                return pattern_name

        return None

    def build_hierarchical_cross_links(
        self, events: list[dict]
    ) -> list[tuple[dict, dict, str, float]]:
        """Generate candidate pairs from hierarchical event patterns."""
        cross_links = []

        for i, event_a in enumerate(events):
            ts_a = self.parse_timestamp(event_a.get("timestamp"))
            if not ts_a:
                continue

            for j in range(i + 1, len(events)):
                event_b = events[j]
                ts_b = self.parse_timestamp(event_b.get("timestamp"))

                if not ts_b or ts_b <= ts_a:
                    continue

                # Check within window
                days_between = (ts_b - ts_a).total_seconds() / 86400
                if days_between > 30:  # Max hierarchical window
                    continue

                hier_match = self.check_hierarchical_pattern(event_a, event_b)
                if hier_match:
                    cross_links.append(
                        (event_a, event_b, f"hierarchical: {hier_match}", 0.15)
                    )

        return cross_links

    # =========================================================================
    # MAIN CANDIDATE PAIR BUILDING
    # =========================================================================

    def build_candidate_pairs(self, events: list[dict]) -> list[dict]:
        """Build all cross-link candidate pairs using multiple strategies."""
        print("[*] Building cross-link candidate pairs...")

        all_pairs = []
        stop_generating = False

        # 1. Entity tracking cross-links
        if ENTITY_TRACKING_ENABLED and not stop_generating:
            print("  [*] Building entity-based cross-links...")
            entity_index = self.build_entity_index(events)
            print(f"      Found {len(entity_index)} entities with multiple occurrences")
            entity_pairs = self.build_entity_cross_links(events, entity_index)
            # Limit per type to prevent memory/CPU overload
            max_per_type = MAX_CANDIDATE_PAIRS // 5
            if len(entity_pairs) > max_per_type:
                entity_pairs = random.sample(entity_pairs, max_per_type)
                print(f"      Limited to {max_per_type} pairs per type")
            print(f"      Generated {len(entity_pairs)} entity-based pairs")
            # Early exit if total would exceed limit
            if len(all_pairs) + len(entity_pairs) > MAX_CANDIDATE_PAIRS:
                print(
                    f"      [!] Reached MAX_CANDIDATE_PAIRS={MAX_CANDIDATE_PAIRS}, stopping early"
                )
                stop_generating = True
            else:
                all_pairs.extend(entity_pairs)
                self.stats["pairs_entity"] = len(entity_pairs)

        # 2. Thematic continuity cross-links
        if THEMATIC_TRACKING_ENABLED and not stop_generating:
            print("  [*] Building thematic continuity cross-links...")
            thematic_pairs = self.build_thematic_cross_links(events)
            # Limit per type to prevent memory/CPU overload
            max_per_type = MAX_CANDIDATE_PAIRS // 5
            if len(thematic_pairs) > max_per_type:
                thematic_pairs = random.sample(thematic_pairs, max_per_type)
                print(f"      Limited to {max_per_type} pairs per type")
            print(f"      Generated {len(thematic_pairs)} thematic pairs")
            # Early exit if total would exceed limit
            if len(all_pairs) + len(thematic_pairs) > MAX_CANDIDATE_PAIRS:
                print(
                    f"      [!] Reached MAX_CANDIDATE_PAIRS={MAX_CANDIDATE_PAIRS}, stopping early"
                )
                stop_generating = True
            else:
                all_pairs.extend(thematic_pairs)
                self.stats["pairs_thematic"] = len(thematic_pairs)

        # 3. Lagged pattern cross-links
        if LAG_PATTERNS_ENABLED and not stop_generating:
            print("  [*] Building lagged pattern cross-links...")
            lag_pairs = self.build_lagged_cross_links(events)
            # Limit per type to prevent memory/CPU overload
            max_per_type = MAX_CANDIDATE_PAIRS // 5
            if len(lag_pairs) > max_per_type:
                lag_pairs = random.sample(lag_pairs, max_per_type)
                print(f"      Limited to {max_per_type} pairs per type")
            print(f"      Generated {len(lag_pairs)} lagged pattern pairs")
            # Early exit if total would exceed limit
            if len(all_pairs) + len(lag_pairs) > MAX_CANDIDATE_PAIRS:
                print(
                    f"      [!] Reached MAX_CANDIDATE_PAIRS={MAX_CANDIDATE_PAIRS}, stopping early"
                )
                stop_generating = True
            else:
                all_pairs.extend(lag_pairs)
                self.stats["pairs_lag"] = len(lag_pairs)

        # 4. Multi-window expansion
        if not stop_generating:
            print("  [*] Building multi-window pairs...")
            multi_window_pairs = self.build_multi_window_pairs(events)
            # Limit per type to prevent memory/CPU overload
            max_per_type = MAX_CANDIDATE_PAIRS // 5
            if len(multi_window_pairs) > max_per_type:
                multi_window_pairs = random.sample(multi_window_pairs, max_per_type)
                print(f"      Limited to {max_per_type} pairs per type")
            print(f"      Generated {len(multi_window_pairs)} multi-window pairs")
            # Early exit if total would exceed limit
            if len(all_pairs) + len(multi_window_pairs) > MAX_CANDIDATE_PAIRS:
                print(
                    f"      [!] Reached MAX_CANDIDATE_PAIRS={MAX_CANDIDATE_PAIRS}, stopping early"
                )
                stop_generating = True
            else:
                all_pairs.extend(multi_window_pairs)
                self.stats["pairs_multi_window"] = len(multi_window_pairs)

        # 5. Hierarchical patterns
        if not stop_generating:
            print("  [*] Building hierarchical pattern cross-links...")
            hier_pairs = self.build_hierarchical_cross_links(events)
            # Limit per type to prevent memory/CPU overload
            max_per_type = MAX_CANDIDATE_PAIRS // 5
            if len(hier_pairs) > max_per_type:
                hier_pairs = random.sample(hier_pairs, max_per_type)
                print(f"      Limited to {max_per_type} pairs per type")
            print(f"      Generated {len(hier_pairs)} hierarchical pairs")
            # Early exit if total would exceed limit
            if len(all_pairs) + len(hier_pairs) > MAX_CANDIDATE_PAIRS:
                print(
                    f"      [!] Reached MAX_CANDIDATE_PAIRS={MAX_CANDIDATE_PAIRS}, stopping early"
                )
                stop_generating = True
            else:
                all_pairs.extend(hier_pairs)
                self.stats["pairs_hierarchical"] = len(hier_pairs)

        # Convert to dict format for easier handling
        candidate_pairs = []
        for event_a, event_b, linking_reason, weight in all_pairs:
            pair_key = f"{event_a.get('id')}->{event_b.get('id')}"
            if pair_key not in self.seen_pair_keys:
                self.seen_pair_keys.add(pair_key)

                # Phase 6: Compute entity Jaccard similarity for each pair
                entities_a = self.extract_entities(event_a.get("content", ""))
                entities_b = self.extract_entities(event_b.get("content", ""))
                entity_jaccard = self.calculate_entity_jaccard(entities_a, entities_b)

                candidate_pairs.append(
                    {
                        "from_event": event_a,
                        "to_event": event_b,
                        "linking_reason": linking_reason,
                        "weight": weight,
                        "pair_key": pair_key,
                        # Phase 6: Entity similarity data
                        "entity_jaccard": entity_jaccard,
                        "entities_a": entities_a,
                        "entities_b": entities_b,
                    }
                )

        # Phase 6: Filter by entity Jaccard threshold if NER is enabled
        if NER_ENABLED and ENTITY_SIMILARITY_THRESHOLD > 0:
            filtered_pairs = []
            for pair in candidate_pairs:
                # Keep pairs that either exceed entity Jaccard threshold
                # OR are from strategies with explicit entity linking (linking_reason contains "entity")
                if (
                    pair["entity_jaccard"] >= ENTITY_SIMILARITY_THRESHOLD
                    or "shared entity" in pair["linking_reason"]
                ):
                    filtered_pairs.append(pair)
                elif VERBOSE:
                    print(
                        f"      [~] Filtered pair by entity Jaccard: {pair['pair_key']} (jaccard={pair['entity_jaccard']:.2f})"
                    )

            print(
                f"      [*] Entity Jaccard filtering: {len(candidate_pairs)} -> {len(filtered_pairs)} pairs"
            )
            candidate_pairs = filtered_pairs

        self.stats["pairs_total"] = len(candidate_pairs)
        print(
            f"[+] Total unique candidate pairs (after filtering): {len(candidate_pairs)}"
        )

        # Apply safety limit
        if len(candidate_pairs) > MAX_CANDIDATE_PAIRS:
            print(
                f"[!] {len(candidate_pairs)} pairs generated, truncating to {MAX_CANDIDATE_PAIRS}"
            )
            # Sort by weight descending and take top N
            candidate_pairs.sort(key=lambda x: x["weight"], reverse=True)
            candidate_pairs = candidate_pairs[:MAX_CANDIDATE_PAIRS]

        return candidate_pairs

    # =========================================================================
    # GEMINI SCORING
    # =========================================================================

    async def score_pair_with_gemini(
        self,
        event_a: dict,
        event_b: dict,
        linking_reason: str,
        client: httpx.AsyncClient,
    ) -> dict:
        """Query Gemini to determine if event A causes event B with cross-link context."""

        prompt = f"""You are a causal analysis expert. Given two events, determine if the first event plausibly caused the second event.
Return ONLY valid JSON: {{"causal": true/false, "confidence": 0.0-1.0, "mechanism": "brief explanation"}}

Event A: {event_a.get("content", "")[:1000]}
Event A timestamp: {event_a.get("timestamp")}

Event B: {event_b.get("content", "")[:1000]}
Event B timestamp: {event_b.get("timestamp")}

LINKING CONTEXT: These events are being evaluated because of: {linking_reason}

Analysis: Did Event A plausibly cause Event B? Consider:
- Temporal precedence (A must come before B)
- Mechanism plausibility (is there a logical causal link?)
- Context: The events share {linking_reason}. Use this context to evaluate plausibility.
- Extended time gap: Unlike same-day events, cross-day links require stronger causal evidence.

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
                    print(f"        [!] Rate limited, waiting {wait_time}s...")
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
                    print(f"        [!] Timeout on attempt {attempt + 1}, retrying...")
                    continue
                return {"success": False, "error": "Request timeout after retries"}
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_BACKOFF * (2**attempt))
                    continue
                return {"success": False, "error": str(e)}

        return {"success": False, "error": "Max retries exceeded"}

    async def score_batch(self, pairs: list[dict]) -> list[dict]:
        """Score a batch of pairs with Gemini."""
        if not pairs:
            return []

        self.gemini_api_key = self.get_gemini_api_key()

        # Load checkpoint if exists
        checkpoint_data = self.load_checkpoint()
        processed_pairs = checkpoint_data.get("processed_pairs", [])
        results = checkpoint_data.get("results_so_far", [])

        # Build set of already processed pair keys for quick lookup
        processed_keys = set()
        for pair in processed_pairs:
            pair_key = f"{pair.get('from_id', '')}->{pair.get('to_id', '')}"
            processed_keys.add(pair_key)

        if processed_keys:
            print(
                f"[*] Resuming from checkpoint: {len(processed_keys)} pairs already processed"
            )

        # Filter out already processed pairs
        remaining_pairs = []
        for i, pair in enumerate(pairs):
            pair_key = f"{pair['from_event'].get('id')}->{pair['to_event'].get('id')}"
            if pair_key not in processed_keys:
                remaining_pairs.append((i, pair))

        if not remaining_pairs:
            print("[*] All pairs already processed")
            return results

        print(f"[*] Processing {len(remaining_pairs)} remaining pairs...")

        # Phase 6: Pre-compute embeddings for thematic similarity
        embedding_cache = {}
        if USE_GEMINI_EMBEDDINGS:
            print(
                f"  [*] Pre-computing embeddings for {len(remaining_pairs) * 2} events..."
            )
            all_events = list(
                {
                    str(p[1]["from_event"].get("id")): p[1]["from_event"]
                    for p in remaining_pairs
                }.values()
            ) + list(
                {
                    str(p[1]["to_event"].get("id")): p[1]["to_event"]
                    for p in remaining_pairs
                }.values()
            )

            async with httpx.AsyncClient() as emb_client:
                embedding_cache = await self.build_embedding_cache(
                    all_events, emb_client
                )

        async with httpx.AsyncClient() as client:
            for pair_idx, (original_idx, pair) in enumerate(remaining_pairs):
                # Check for shutdown request
                if self.shutdown_requested:
                    print(
                        f"\n[!] Shutdown requested during scoring. Saving checkpoint..."
                    )
                    all_processed = processed_pairs + [
                        {
                            "from_id": p["from_event"].get("id"),
                            "to_id": p["to_event"].get("id"),
                        }
                        for _, p in remaining_pairs[:pair_idx]
                    ]
                    self.save_checkpoint(all_processed, results)
                    print(
                        f"[*] Checkpoint saved. Processed {len(results)} pairs so far."
                    )
                    print("[*] Run script again to resume from checkpoint.")
                    break

                event_a = pair["from_event"]
                event_b = pair["to_event"]
                linking_reason = pair["linking_reason"]
                entity_jaccard = pair.get("entity_jaccard", 0.0)

                print(f"  [*] Scoring pair {pair_idx + 1}/{len(remaining_pairs)}...")
                print(f"      A: {event_a.get('id')} ({event_a.get('timestamp')})")
                print(f"      B: {event_b.get('id')} ({event_b.get('timestamp')})")
                print(f"      Reason: {linking_reason}")
                print(f"      Entity Jaccard: {entity_jaccard:.2f}")

                result = await self.score_pair_with_gemini(
                    event_a, event_b, linking_reason, client
                )
                result["from_event"] = event_a
                result["to_event"] = event_b
                result["linking_reason"] = linking_reason
                result["weight"] = pair["weight"]
                result["entity_jaccard"] = entity_jaccard

                # Phase 6: Compute thematic similarity using cached embeddings
                thematic_similarity = 0.0
                thematic_match = False

                if USE_GEMINI_EMBEDDINGS and embedding_cache:
                    event_id_a = str(event_a.get("id"))
                    event_id_b = str(event_b.get("id"))

                    emb_a = embedding_cache.get(event_id_a)
                    emb_b = embedding_cache.get(event_id_b)

                    if emb_a and emb_b:
                        thematic_similarity = self.cosine_similarity(emb_a, emb_b)
                        thematic_match = (
                            thematic_similarity >= GEMINI_EMBEDDING_THRESHOLD
                        )
                        print(
                            f"      Thematic Similarity: {thematic_similarity:.2f} (threshold: {GEMINI_EMBEDDING_THRESHOLD})"
                        )

                result["thematic_similarity"] = thematic_similarity
                result["thematic_match"] = thematic_match

                # Phase 6: Apply combined confidence boost
                # Boost if both entity Jaccard and thematic similarity pass thresholds
                if thematic_match and entity_jaccard >= ENTITY_SIMILARITY_THRESHOLD:
                    result["entity_thematic_boost"] = ENTITY_THEMATIC_BOOST
                    print(
                        f"      [+] Entity + Thematic boost applied: +{ENTITY_THEMATIC_BOOST}"
                    )
                else:
                    result["entity_thematic_boost"] = 0.0

                results.append(result)

                if result.get("success"):
                    is_causal = result.get("causal", False)
                    conf = result.get("confidence", 0.0)
                    print(f"      [?] Causal: {is_causal}, Conf: {conf:.2f}")
                else:
                    print(f"      [!] Failed: {result.get('error')}")

                # Save checkpoint every 50 pairs
                if (pair_idx + 1) % 50 == 0:
                    all_processed = processed_pairs + [
                        {
                            "from_id": p["from_event"].get("id"),
                            "to_id": p["to_event"].get("id"),
                        }
                        for _, p in remaining_pairs[: pair_idx + 1]
                    ]
                    self.save_checkpoint(all_processed, results)
                    print(f"[*] Intermediate checkpoint saved at pair {pair_idx + 1}")

                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)

        return results

    # =========================================================================
    # DATABASE OPERATIONS
    # =========================================================================

    async def check_link_exists(self, from_id: str, to_id: str) -> bool:
        """Check if causal link already exists in database."""
        query = (
            "SELECT id FROM causal_link WHERE from = $from_id AND to = $to_id LIMIT 1;"
        )
        try:
            result = await self.db.query(query, {"from_id": from_id, "to_id": to_id})
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
        provenance: dict,
    ) -> bool:
        """Write a causal link to SurrealDB."""
        run_time = datetime.now(timezone.utc).isoformat()

        # Format strings for SQL
        clean_mechanism = mechanism.replace('"', '\\"').replace("\n", " ")

        provenance_json = json.dumps(provenance).replace('"', '\\"')

        query = f"""
            CREATE causal_link SET
            from = {from_id},
            to = {to_id},
            confidence = {confidence},
            mechanism = "{clean_mechanism}",
            evidence = [],
            provenance = {provenance_json};
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

    def get_strategy_from_reason(self, linking_reason: str) -> str:
        """Extract strategy name from linking reason."""
        if linking_reason.startswith("shared entity"):
            return "entity"
        elif linking_reason.startswith("thematic"):
            return "thematic"
        elif linking_reason.startswith("lagged"):
            return "lag"
        elif linking_reason.startswith("multi-window"):
            return "multi_window"
        elif linking_reason.startswith("hierarchical"):
            return "hierarchical"
        return "unknown"

    def get_window_hours_from_reason(self, linking_reason: str) -> int:
        """Extract window hours from linking reason."""
        if "24h" in linking_reason:
            return 24
        elif "72h" in linking_reason:
            return 72
        elif "168h" in linking_reason:
            return 168
        elif "720h" in linking_reason:
            return 720
        elif "short_term" in linking_reason:
            return 72
        elif "medium_term" in linking_reason:
            return 168
        elif "long_term" in linking_reason:
            return 720
        return 0

    # =========================================================================
    # MAIN PROCESSING
    # =========================================================================

    async def process_pairs(self, pairs: list[dict]) -> list[dict]:
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

            # Apply confidence threshold (with weight boost for cross-links)
            causal = result.get("causal", False)
            base_confidence = result.get("confidence", 0.0)
            weight = result.get("weight", 0.0)
            linking_reason = result.get("linking_reason", "")
            entity_jaccard = result.get("entity_jaccard", 0.0)
            thematic_similarity = result.get("thematic_similarity", 0.0)
            entity_thematic_boost = result.get("entity_thematic_boost", 0.0)

            # Apply weight as a boost to effective confidence
            effective_confidence = min(
                base_confidence + (weight * 0.1) + entity_thematic_boost, 1.0
            )

            if not causal or effective_confidence < CONFIDENCE_THRESHOLD:
                self.stats["links_rejected"] += 1
                self.rejected_pairs.append(
                    {
                        "from": from_id,
                        "to": to_id,
                        "confidence": effective_confidence,
                        "reason": "below_threshold" if not causal else "low_confidence",
                    }
                )
                continue

            # Check for duplicates (already processed in this run)
            pair_key = f"{from_id}->{to_id}"
            if pair_key in self.seen_pair_keys:
                self.stats["links_duplicated"] += 1
                continue
            self.seen_pair_keys.add(pair_key)

            # Build provenance
            provenance = {
                "model": GEMINI_MODEL,
                "run_time": datetime.now(timezone.utc).isoformat(),
                "strategy": self.get_strategy_from_reason(linking_reason),
                "linking_reason": linking_reason,
                "window_hours": self.get_window_hours_from_reason(linking_reason),
                "base_confidence": base_confidence,
                "weight_boost": weight,
                # Phase 6: NER and embedding metadata
                "entity_jaccard": entity_jaccard,
                "thematic_similarity": thematic_similarity,
                "entity_thematic_boost": entity_thematic_boost,
            }

            # Final check against DB
            if not DRY_RUN:
                exists = await self.check_link_exists(from_id, to_id)
                if exists:
                    print(f"      [-] Link already exists in DB: {from_id} -> {to_id}")
                    self.stats["links_duplicated"] += 1
                    continue

                # Write to database
                success = await self.write_causal_link(
                    from_id,
                    to_id,
                    effective_confidence,
                    result.get("mechanism", ""),
                    provenance,
                )
                if success:
                    print(f"      [+] Link accepted and written: {from_id} -> {to_id}")
                    self.stats["links_accepted"] += 1
                    self.stats["db_writes"] += 1
                    accepted.append(
                        {
                            "from": from_id,
                            "to": to_id,
                            "confidence": effective_confidence,
                            "mechanism": result.get("mechanism", ""),
                            "provenance": provenance,
                        }
                    )
            else:
                print(f"      [+] Link accepted (Dry Run): {from_id} -> {to_id}")
                print(f"          Provenance: {json.dumps(provenance, indent=4)}")
                self.stats["links_accepted"] += 1
                accepted.append(
                    {
                        "from": from_id,
                        "to": to_id,
                        "confidence": effective_confidence,
                        "mechanism": result.get("mechanism", ""),
                        "provenance": provenance,
                    }
                )

        return accepted

    async def run(self) -> dict:
        """Execute the full cross-link discovery pipeline."""
        print("=" * 60)
        print("Volva Cross-Link Discovery - Phase 6 (NER + Thematic Embeddings)")
        print("=" * 60)
        print(f"Mode: {'DRY_RUN' if DRY_RUN else 'PRODUCTION'}")
        print(f"Max candidate pairs: {MAX_CANDIDATE_PAIRS}")
        print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
        print(f"NER enabled: {NER_ENABLED}")
        print(f"Gemini embeddings enabled: {USE_GEMINI_EMBEDDINGS}")
        print()

        await self.connect()

        try:
            # Load events within extended window
            events = await self.load_events(days=CROSS_LINK_WINDOW_DAYS)
            if not events:
                print("[!] No events found in database")
                return {"stats": self.stats, "accepted_links": []}

            # Build candidate pairs using multiple strategies
            pairs = self.build_candidate_pairs(events)
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
            print(f"Entity pairs: {self.stats['pairs_entity']}")
            print(f"Thematic pairs: {self.stats['pairs_thematic']}")
            print(f"Lagged pairs: {self.stats['pairs_lag']}")
            print(f"Multi-window pairs: {self.stats['pairs_multi_window']}")
            print(f"Hierarchical pairs: {self.stats['pairs_hierarchical']}")
            print(f"Total pairs: {self.stats['pairs_total']}")
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
    """Entry point."""
    if not CROSS_LINK_ENABLED:
        print("[!] Cross-link discovery is disabled in configuration")
        sys.exit(1)

    linker = CrossLinker()

    try:
        result = await linker.run()

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
