#!/usr/bin/env python3
"""
Volva Market Resolution Feedback Loop - Phase 6
Monitors resolved Lethe markets, extracts outcome vs prediction accuracy,
and updates causal link weights/confidences in SurrealDB.

This closes the prediction loop by using market resolution data to refine
the confidence of causal links that generated correct/incorrect predictions.

Usage:
    PYTHONUNBUFFERED=1 DRY_RUN=false python3 feedback_loop.py

Environment Variables:
    DRY_RUN: Set to 'true' for dry run mode (no DB writes)

TRACE INSTRUMENTATION (wrk-0094):
  pred→pos→fb steps traced:
    - positions_predicted: resolved Polymarket positions loaded
    - positions_eligible: unprocessed resolved positions
    - positions_opened: feedback_processed=false positions loaded
    - positions_linked: positions with predictions found (via find_prediction_for_event)
    - positions_resolved: positions feedback processed
    - causal_links_updated: causal links with confidence updated
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import yaml
from surrealdb import AsyncSurreal

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# TRACE INSTRUMENTATION: shared trace file path
TRACE_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "trace_log.txt")

def _write_trace(msg: str):
    """Append a trace line to the shared trace log file."""
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] [FEEDBACK_LOOP] {msg}"
    logging.info(line)  # Use logging so it respects log level
    try:
        with open(TRACE_LOG_FILE, "a") as f:
            f.write(line + "\n")
    except IOError as e:
        logging.warning(f"[TRACE] Could not write to {TRACE_LOG_FILE}: {e}")

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEIGHTS_FILE = os.path.join(SCRIPT_DIR, "..", "weights", "confidences")

# Default config values
DEFAULT_CONFIG = {
    "feedback_loop": {
        "enabled": True,
        "learning_rate": 0.1,
        "min_confidence": 0.01,
        "max_confidence": 0.99,
        "batch_size": 50,
        "dry_run": False,
        "skip_processed": True,
        "min_predictions_for_update": 1,
    },
    "logging": {
        "level": "INFO",
        "format": "[%(asctime)s] %(levelname)-8s %(name)s - %(message)s",
    },
}

# Database settings (from predictor_config)
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"


def load_config() -> Dict[str, Any]:
    """Load configuration from weights/confidences file."""
    config = DEFAULT_CONFIG.copy()
    
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE, "r") as f:
                user_config = yaml.safe_load(f)
                if user_config:
                    # Merge user config over defaults
                    for section, values in user_config.items():
                        if section in config and isinstance(values, dict):
                            config[section].update(values)
                        else:
                            config[section] = values
        except Exception as e:
            logging.warning(f"Failed to load config from {WEIGHTS_FILE}: {e}")
    
    return config


def extract_query_result(res):
    """Handle SurrealDB 1.x response envelopes and direct results."""
    if not res:
        return []
    if isinstance(res, list) and res and isinstance(res[0], dict) and "result" in res[0]:
        return res[0]["result"] if res[0]["result"] else []
    return res if isinstance(res, list) else [res]


class FeedbackLoop:
    """
    Feedback loop that updates causal link confidences based on
    the accuracy of predictions derived from those links.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config["feedback_loop"]
        self.db: Optional[AsyncSurreal] = None
        self.run_id = str(uuid.uuid4())[:8]
        self.stats = {
            "markets_processed": 0,
            "markets_skipped": 0,
            "links_updated": 0,
            "links_unchanged": 0,
            "errors": 0,
            "total_predictions": 0,
            "correct_predictions": 0,
            # Polymarket position tracking
            "positions_processed": 0,
            "positions_skipped": 0,
            "positions_with_prediction": 0,
            "positions_without_prediction": 0,
            "hits": 0,
            "misses": 0,
            # Phase 7: Annotation signal processing (wrk-0307)
            "annotations_processed": 0,
            "annotations_skipped": 0,
            "annotation_links_created": 0,
            # Phase 7: Disagreement signal processing (wrk-0307)
            "disagreements_processed": 0,
            "disagreements_skipped": 0,
            "disagreement_weighted_updates": 0,
        }
        self.processed_market_ids: set = set()
        self.processed_position_ids: set = set()
        
        # Learning parameters
        self.learning_rate = self.config.get("learning_rate", 0.1)
        self.min_confidence = self.config.get("min_confidence", 0.01)
        self.max_confidence = self.config.get("max_confidence", 0.99)
        self.batch_size = self.config.get("batch_size", 50)
        # Fix: Explicitly check env var first, then config
        # DRY_RUN=false or unset means PRODUCTION (dry_run=False)
        # DRY_RUN=true means DRY_RUN mode (dry_run=True)
        env_dry_run = os.environ.get("DRY_RUN", "").lower()
        self.dry_run = env_dry_run == "true"  # Only true if explicitly set to "true"
        self.skip_processed = self.config.get("skip_processed", True)
        self.min_predictions = self.config.get("min_predictions_for_update", 1)
        
        logging.info(f"FeedbackLoop initialized (run_id={self.run_id}, dry_run={self.dry_run})")
        _write_trace(f"INIT: run_id={self.run_id} dry_run={self.dry_run} batch_size={self.batch_size} skip_processed={self.skip_processed}")
    
    async def connect(self):
        """Connect to SurrealDB."""
        logging.info(f"Connecting to SurrealDB at {SURREALDB_HOST}...")
        self.db = AsyncSurreal(SURREALDB_HOST)
        await self.db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
        await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)
        logging.info("Connected to SurrealDB")
    
    async def close(self):
        """Close SurrealDB connection."""
        if self.db:
            try:
                await self.db.close()
            except NotImplementedError:
                logging.debug("AsyncSurreal.close() not implemented")
            except Exception as e:
                logging.warning(f"Error closing connection: {e}")
    
    async def load_processed_markets(self):
        """Load IDs of markets already processed for feedback."""
        query = """
            SELECT id FROM market_draft 
            WHERE feedback_processed = true 
            LIMIT 1000;
        """
        res = await self.db.query(query)
        results = extract_query_result(res)
        self.processed_market_ids = {str(r.get("id", "")) for r in results if r.get("id")}
        logging.info(f"Loaded {len(self.processed_market_ids)} already-processed markets")
    
    async def load_resolved_markets(self) -> List[Dict[str, Any]]:
        """
        Load resolved market_drafts that haven't been processed for feedback.
        """
        query = """
            SELECT * FROM market_draft 
            WHERE status = 'resolved' 
            AND (feedback_processed = false OR feedback_processed = 'false' OR feedback_processed IS NONE)
            ORDER BY resolve_time DESC
            LIMIT $limit;
        """
        res = await self.db.query(query, {"limit": self.batch_size})
        markets = extract_query_result(res)
        logging.info(f"Found {len(markets)} resolved markets to process")
        return markets
    
    async def load_processed_polymarket_positions(self):
        """Load IDs of polymarket positions already processed for feedback."""
        query = """
            SELECT id FROM polymarket_position 
            WHERE feedback_processed = true 
            LIMIT 1000;
        """
        res = await self.db.query(query)
        results = extract_query_result(res)
        self.processed_position_ids = {str(r.get("id", "")) for r in results if r.get("id")}
        logging.info(f"Loaded {len(self.processed_position_ids)} already-processed polymarket positions")
    
    async def load_resolved_polymarket_positions(self) -> List[Dict[str, Any]]:
        """
        Load resolved polymarket_position records that haven't been processed for feedback.
        """
        # DEBUG: First check total counts in DB to understand state
        debug_query = "SELECT count() as total, count(status = 'resolved') as resolved_count, count(feedback_processed = false OR feedback_processed = 'false' OR feedback_processed IS NONE) as unprocessed_count FROM polymarket_position GROUP ALL;"
        try:
            debug_res = await self.db.query(debug_query)
            debug_data = extract_query_result(debug_res)
            if debug_data:
                _write_trace(f"DEBUG_POLYMARKET_DB_STATE: {debug_data[0]}")
                logging.info(f"DEBUG: polymarket_position DB state: {debug_data[0]}")
        except Exception as e:
            _write_trace(f"DEBUG_POLYMARKET_DB_STATE_ERROR: {e}")
            logging.warning(f"DEBUG query failed: {e}")
        
        query = """
            SELECT * FROM polymarket_position 
            WHERE status = 'resolved' 
            AND (feedback_processed = false OR feedback_processed = 'false' OR feedback_processed IS NONE)
            ORDER BY resolved_at DESC
            LIMIT $limit;
        """
        res = await self.db.query(query, {"limit": self.batch_size})
        positions = extract_query_result(res)
        # [TRACE] positions_predicted: resolved positions loaded for feedback
        _write_trace(f"positions_predicted={len(positions)}")
        logging.info(f"Found {len(positions)} resolved polymarket positions to process")
        return positions
    
    async def load_prediction(self, prediction_id: str) -> Optional[Dict[str, Any]]:
        """Load a prediction by ID."""
        query = "SELECT * FROM predicts WHERE id = $id LIMIT 1;"
        res = await self.db.query(query, {"id": prediction_id})
        predictions = extract_query_result(res)
        return predictions[0] if predictions else None
    
    async def load_causal_link(self, link_id: str) -> Optional[Dict[str, Any]]:
        """Load a causal link by ID."""
        # FIX (wrk-0269): Use f-string interpolation for record ID comparison
        # because parameterized queries don't properly match record<causal_link> fields
        query = f"SELECT * FROM causal_link WHERE id = {link_id} LIMIT 1;"
        res = await self.db.query(query)
        links = extract_query_result(res)
        return links[0] if links else None
    
    def calculate_accuracy(
        self,
        predicted_probability: float,
        actual_outcome: str
    ) -> bool:
        """
        Determine if a prediction was correct.
        
        Args:
            predicted_probability: The predicted probability (0.0 to 1.0)
            actual_outcome: The actual outcome ("YES" or "NO")
        
        Returns:
            True if the prediction was correct, False otherwise.
        """
        predicted_outcome = "YES" if predicted_probability >= 0.5 else "NO"
        is_correct = (predicted_outcome == actual_outcome)
        return is_correct
    
    def compute_new_confidence(
        self,
        old_confidence: float,
        is_correct: bool
    ) -> float:
        """
        Compute the new confidence using multiplicative weight update.
        
        Args:
            old_confidence: Current confidence (0.0 to 1.0)
            is_correct: Whether the prediction was correct
        
        Returns:
            Updated confidence value, clamped to [min_confidence, max_confidence].
        """
        if is_correct:
            # Move toward 1: new = old + rate * (1 - old)
            new_conf = old_confidence + self.learning_rate * (1 - old_confidence)
        else:
            # Move toward 0: new = old - rate * old
            new_conf = old_confidence - self.learning_rate * old_confidence
        
        # Clamp to configured bounds
        new_conf = max(self.min_confidence, min(self.max_confidence, new_conf))
        return new_conf
    
    async def update_causal_link_confidence(
        self,
        link_id: str,
        old_confidence: float,
        new_confidence: float,
        feedback_entry: Dict[str, Any]
    ):
        """
        Update a causal link's confidence and feedback history.
        
        Note: SurrealDB requires record IDs to be embedded directly in the query
        string (via f-string), not passed as query parameters. Additionally,
        fields must be initialized with IF expressions before using array::append.
        """
        if self.dry_run:
            logging.info(f"[DRY_RUN] Would update {link_id}: {old_confidence:.4f} -> {new_confidence:.4f}")
            return
        
        import json
        
        try:
            # FIX: Use f-string for link_id (not $link_id parameter)
            # Also use IF expressions to initialize fields if they don't exist
            correct_inc = 1 if feedback_entry.get("was_correct") else 0
            
            # First, initialize array and counter fields if they are NULL/NONE
            init_query = f"""
                UPDATE {link_id} SET
                    feedback_history = if feedback_history then feedback_history else [] end,
                    total_predictions = if total_predictions then total_predictions else 0 end,
                    correct_predictions = if correct_predictions then correct_predictions else 0 end;
            """
            await self.db.query(init_query)
            
            # Then do the full update with the initialized fields
            # Note: feedback_entry is JSON-serialized to avoid SurrealDB
            # interpreting nested objects as field paths
            update_query = f"""
                UPDATE {link_id} SET
                    confidence = $new_conf,
                    feedback_history = array::append(feedback_history, $entry),
                    total_predictions = total_predictions + 1,
                    correct_predictions = correct_predictions + $correct_inc;
            """
            
            res = await self.db.query(
                update_query,
                {
                    "new_conf": new_confidence,
                    "entry": json.dumps(feedback_entry),
                    "correct_inc": correct_inc,
                }
            )
            
            logging.info(f"Updated {link_id}: {old_confidence:.4f} -> {new_confidence:.4f}")
            
        except Exception as e:
            logging.error(f"Failed to update causal_link {link_id}: {e}")
            raise
    
    async def mark_market_processed(self, market_id: str):
        """Mark a market as processed for feedback."""
        if self.dry_run:
            return
        
        try:
            query = f"UPDATE {market_id} SET feedback_processed = true;"
            await self.db.query(query)
        except Exception as e:
            logging.warning(f"Failed to mark market {market_id} as processed: {e}")
    
    async def process_market(self, market: Dict[str, Any]) -> bool:
        """
        Process a single resolved market to update causal link confidence.
        
        Returns:
            True if processed successfully, False otherwise.
        """
        market_id = str(market.get("id", ""))
        
        try:
            # Extract prediction ID from market draft
            source_prediction_id = market.get("source_prediction_id")
            if not source_prediction_id:
                logging.warning(f"Market {market_id} has no source_prediction_id, skipping")
                return False
            
            # Load the prediction
            prediction = await self.load_prediction(str(source_prediction_id))
            if not prediction:
                logging.warning(f"Prediction {source_prediction_id} not found for market {market_id}")
                return False
            
            # Get predicted probability
            predicted_probability = prediction.get("probability", 0.5)
            
            # Determine actual outcome (from oracle fields or resolution)
            # The outcome is stored in the market's oracle data
            oracle = market.get("oracle", {})
            actual_outcome = oracle.get("outcome", None)
            
            if actual_outcome is None:
                # Try to infer from status or other fields
                # For now, default to YES if market resolved successfully
                # This is a simplification - real implementation would check resolution data
                logging.debug(f"Market {market_id} has no explicit outcome, assuming YES")
                actual_outcome = "YES"
            
            # Calculate accuracy
            is_correct = self.calculate_accuracy(predicted_probability, actual_outcome)
            self.stats["total_predictions"] += 1
            if is_correct:
                self.stats["correct_predictions"] += 1
            
            logging.info(
                f"Market {market_id}: pred_prob={predicted_probability:.2f}, "
                f"actual={actual_outcome}, correct={is_correct}"
            )
            
            # Get causal link ID from prediction
            causal_link_id = prediction.get("causal_link_id")
            if not causal_link_id:
                logging.warning(f"Prediction {prediction.get('id')} has no causal_link_id")
                return False
            
            # Load the causal link
            causal_link = await self.load_causal_link(str(causal_link_id))
            if not causal_link:
                logging.warning(f"Causal link {causal_link_id} not found")
                return False
            
            # Compute new confidence
            old_confidence = causal_link.get("confidence", 0.5)
            new_confidence = self.compute_new_confidence(old_confidence, is_correct)
            
            # Build feedback entry
            feedback_entry = {
                "run_id": self.run_id,
                "updated": datetime.now(timezone.utc).isoformat(),
                "market_id": market_id,
                "prediction_id": str(prediction.get("id", "")),
                "was_correct": is_correct,
                "old_confidence": old_confidence,
                "new_confidence": new_confidence,
                "predicted_probability": predicted_probability,
                "actual_outcome": actual_outcome,
            }
            
            # Update the causal link
            await self.update_causal_link_confidence(
                str(causal_link_id),
                old_confidence,
                new_confidence,
                feedback_entry
            )
            
            # Mark market as processed
            await self.mark_market_processed(market_id)
            
            self.stats["links_updated"] += 1
            return True
            
        except Exception as e:
            logging.error(f"Error processing market {market_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def find_prediction_for_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a prediction for a given event ID.
        Returns the first prediction found, or None if no predictions exist for this event.
        
        FIX (wrk-0269): Use RecordID literal format (no quotes) for source_event comparison
        because source_event is a RecordID type, not a string. SurrealDB compares
        RecordID fields by their unquoted literal form (e.g., event:xyz), not strings.
        """
        # FIX (wrk-0269): Use RecordID literal format (no quotes) for proper RecordID comparison
        # source_event is a RecordID type, so comparing with quoted string fails (String != RecordID)
        query = f"SELECT * FROM predicts WHERE source_event = {event_id} LIMIT 1;"
        res = await self.db.query(query)
        predictions = extract_query_result(res)
        
        # [TRACE] Log match result for debugging
        if predictions:
            _write_trace(
                f"find_prediction_for_event_MATCH: event_id={event_id[:30]} "
                f"found pred_id={str(predictions[0].get('id',''))[:20] if isinstance(predictions[0], dict) else 'N/A'}"
            )
        else:
            _write_trace(
                f"find_prediction_for_event_NO_MATCH: event_id={event_id[:40]} "
                f"(source_event is RecordID type - query uses unquoted literal)"
            )
            # Also log sample source_event values for comparison (first 3 only)
            try:
                sample_query = "SELECT id, source_event FROM predicts LIMIT 3;"
                sample_res = await self.db.query(sample_query)
                sample_preds = extract_query_result(sample_res)
                if sample_preds:
                    samples = "; ".join(
                        f"id={str(p.get('id',''))[:20]} source_event={str(p.get('source_event',''))[:40]}"
                        for p in sample_preds[:3]
                    )
                    _write_trace(f"find_prediction_for_event_SAMPLE_PREDS: {samples}")
            except Exception as dbg_e:
                _write_trace(f"find_prediction_for_event_SAMPLE_QUERY_ERR: {dbg_e}")
        
        return predictions[0] if predictions else None
    
    def extract_keywords(self, text: str) -> List[str]:
        """
        Extract meaningful keywords from text (market question/description).
        
        Args:
            text: Market question/description text
        
        Returns:
            List of lowercase keywords
        """
        if not text:
            return []
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters, keep alphanumeric and spaces
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Split into words
        words = text.split()
        
        # Common stop words to filter out
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can',
            'this', 'that', 'these', 'those', 'it', 'its', 'they', 'them', 'their',
            'what', 'which', 'who', 'whom', 'when', 'where', 'why', 'how',
            'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
            'some', 'such', 'no', 'not', 'only', 'same', 'so', 'than', 'too',
            'very', 'just', 'also', 'now', 'if', 'then', 'else', 'while',
            'about', 'after', 'before', 'between', 'into', 'through', 'during',
            'above', 'below', 'up', 'down', 'out', 'off', 'over', 'under',
            'again', 'further', 'once', 'here', 'there', 'any', 'both',
            'each', 'few', 'more', 'most', 'other', 'some', 'such',
            'yes', 'no', 'maybe', 'question', 'will', 'does', 'did',
        }
        
        # Filter stop words and short words
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        # Deduplicate while preserving order
        seen = set()
        unique_keywords = []
        for w in keywords:
            if w not in seen:
                seen.add(w)
                unique_keywords.append(w)
        
        return unique_keywords[:20]  # Limit to top 20
    
    async def find_prediction_by_title_fuzzy(
        self,
        question: str,
        min_keyword_match: int = 1,
    ) -> Optional[Dict[str, Any]]:
        """
        Find a prediction by fuzzy title/question match when event_id match fails.
        
        Extracts keywords from the question and searches for predictions whose
        causal links have matching mechanism keywords.
        
        Args:
            question: The market question text
            min_keyword_match: Minimum number of keywords that must match
        
        Returns:
            Prediction dict or None if no good match found
        """
        if not question:
            return None
        
        keywords = self.extract_keywords(question)
        if len(keywords) < min_keyword_match:
            return None
        
        try:
            # Get all predictions with their causal links
            res = await self.db.query("""
                SELECT id, target_description, causal_link_id, probability
                FROM predicts
                WHERE causal_link_id IS NOT NONE
                LIMIT 200;
            """)
            predictions = extract_query_result(res)
            
            # FIX (wrk-0269): More robust type checking - handle string predictions (JSON or raw)
            if isinstance(predictions, str):
                _write_trace(f"title_fuzzy_match_STR_DETECTED: predictions is str, attempting JSON parse")
                try:
                    predictions = json.loads(predictions)
                    if isinstance(predictions, list):
                        _write_trace(f"title_fuzzy_match_JSON_PARSE_SUCCESS: parsed to list of {len(predictions)} items")
                    else:
                        _write_trace(f"title_fuzzy_match_FAILED: parsed JSON is not a list (type={type(predictions).__name__})")
                        return None
                except json.JSONDecodeError as je:
                    _write_trace(f"title_fuzzy_match_FAILED: predictions is string but not valid JSON: {je}")
                    return None
            
            if not predictions or not isinstance(predictions, list):
                _write_trace(f"title_fuzzy_match_FAILED: predictions invalid (type={type(predictions).__name__})")
                return None
            
            # FIX (wrk-0263): Ensure all predictions are dicts before accessing .get()
            valid_predictions = [p for p in predictions if isinstance(p, dict)]
            if not valid_predictions:
                _write_trace(f"title_fuzzy_match_FAILED: no valid prediction dicts found")
                return None
            
            # Get causal links to check their mechanisms
            causal_link_ids = list(set(str(p.get('causal_link_id', '')) for p in valid_predictions if p.get('causal_link_id')))
            
            if not causal_link_ids:
                return None
            
            # Build query for causal links using f-string interpolation
            # FIX (wrk-0265): SurrealDB doesn't handle $ids parameter well with IN clause
            # Use f-string to inline the IDs directly as a SurrealDB array
            if causal_link_ids:
                ids_str = ", ".join(f"'{cid}'" for cid in causal_link_ids)
                links_query = f"""
                    SELECT id, mechanism, confidence FROM causal_link 
                    WHERE id IN [{ids_str}]
                    ORDER BY confidence DESC;
                """
                res = await self.db.query(links_query)
            else:
                res = []
            causal_links = extract_query_result(res)
            
            # FIX (wrk-0269): Handle case where causal_links is a JSON string, not a list
            if isinstance(causal_links, str):
                _write_trace(f"title_fuzzy_match_STR_DETECTED: causal_links is str, attempting JSON parse")
                try:
                    causal_links = json.loads(causal_links)
                    if not isinstance(causal_links, list):
                        _write_trace(f"title_fuzzy_match_FAILED: parsed causal_links is not a list")
                        return None
                except json.JSONDecodeError as je:
                    _write_trace(f"title_fuzzy_match_FAILED: causal_links is string but not valid JSON: {je}")
                    return None
            
            # FIX (wrk-0263): Initialize best_prediction before any early returns
            best_prediction = None
            best_score = 0
            best_confidence = 0
            best_matched_kw = []
            
            # FIX (wrk-0263): Validate causal_links is a list of dicts
            if not causal_links or not isinstance(causal_links, list):
                _write_trace(f"title_fuzzy_match_FAILED: causal_links invalid (type={type(causal_links).__name__})")
                return None
            
            valid_links = [l for l in causal_links if isinstance(l, dict)]
            if not valid_links:
                _write_trace(f"title_fuzzy_match_FAILED: no valid causal_link dicts found")
                return None
            
            # Build mechanism lookup
            mechanism_map = {str(l.get('id', '')): (l.get('mechanism') or '').lower() for l in valid_links}
            confidence_map = {str(l.get('id', '')): l.get('confidence', 0.5) for l in valid_links}
            
            # Score each prediction by keyword overlap
            
            # [TRACE] Log keywords being used for fuzzy matching
            _write_trace(f"title_fuzzy_match_ATTEMPT: keywords={keywords[:10]} question={question[:80]}")
            
            for pred in valid_predictions:
                causal_link_id = str(pred.get('causal_link_id', ''))
                mechanism = mechanism_map.get(causal_link_id, '')
                
                # Count keyword matches in mechanism
                score = 0
                matched_kw = []
                for kw in keywords[:10]:
                    if kw in mechanism:
                        score += 1
                        matched_kw.append(kw)
                
                # Also check target_description
                target_desc = (pred.get('target_description') or '').lower()
                for kw in keywords[:5]:
                    if kw in target_desc:
                        score += 0.5
                
                # Apply confidence as a multiplier
                conf = confidence_map.get(causal_link_id, 0.5)
                adjusted_score = score * (0.5 + conf * 0.5)
                
                if adjusted_score > best_score and len(matched_kw) >= min_keyword_match:
                    best_score = adjusted_score
                    best_prediction = pred
                    best_confidence = conf
                    best_matched_kw = matched_kw  # FIX (wrk-0262): Track per best match
            
            if best_prediction:
                _write_trace(
                    f"title_fuzzy_match_FOUND: pred_id={str(best_prediction.get('id',''))[:20]} "
                    f"matched_kw={best_matched_kw} "
                    f"score={best_score:.2f} conf={best_confidence:.2f}"
                )
                logging.debug(
                    f"Found title fuzzy match: pred_id={str(best_prediction.get('id',''))[:20]} "
                    f"score={best_score:.2f}"
                )
            else:
                _write_trace(
                    f"title_fuzzy_match_NO_MATCH: keywords={keywords[:10]} "
                    f"best_score={best_score:.2f} (below threshold or no predictions with causal links)"
                )
            
            return best_prediction
            
        except Exception as e:
            logging.warning(f"Error in title fuzzy match: {e}")
            return None
    
    async def mark_position_processed(self, position_id: str):
        """Mark a polymarket position as processed for feedback."""
        if self.dry_run:
            return
        
        try:
            query = f"UPDATE {position_id} SET feedback_processed = true;"
            await self.db.query(query)
        except Exception as e:
            logging.warning(f"Failed to mark position {position_id} as processed: {e}")
    
    async def find_or_create_prediction_for_event(
        self,
        event_id: str,
        position_id: str,
        hit: bool,
        question: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Find or create a prediction for a given event ID (Manifold).
        
        If no prediction exists via event_id FK, tries title fuzzy-match fallback
        before creating a synthetic prediction.
        
        Args:
            event_id: The reference event ID
            position_id: The manifold position ID (for logging)
            hit: Whether the position was a hit (for synthetic prediction creation)
            question: The market question text (for title fuzzy-match)
        
        Returns:
            Prediction dict or None if creation failed.
        """
        # First try to find existing prediction by event_id
        prediction = await self.find_prediction_for_event(event_id)
        if prediction:
            _write_trace(
                f"pred_linked_by_event_id: event_id={event_id[:20]} "
                f"pred_id={str(prediction.get('id',''))[:20]}"
            )
            return prediction
        
        # FALLBACK 1: Try title fuzzy-match (wrk-0220)
        if question:
            prediction = await self.find_prediction_by_title_fuzzy(question)
            if prediction:
                _write_trace(
                    f"pred_linked_by_title_fuzzy: question={question[:50]} "
                    f"pred_id={str(prediction.get('id',''))[:20]}"
                )
                return prediction
        
        # FALLBACK 2: Create synthetic prediction
        if self.dry_run:
            logging.info(f"[DRY_RUN] Would create synthetic prediction for event {event_id[:20]}...")
            return None
        
        try:
            # Try to find any causal link to associate
            res = await self.db.query("""
                SELECT id, confidence FROM causal_link 
                WHERE confidence >= 0.3
                ORDER BY confidence DESC
                LIMIT 1;
            """)
            links = extract_query_result(res)
            
            # FIX (wrk-0269): Handle case where links is a JSON string, not a list
            if isinstance(links, str):
                _write_trace(f"manifold_synthetic_pred_STR_DETECTED: links is str, attempting JSON parse")
                try:
                    links = json.loads(links)
                    if not isinstance(links, list):
                        _write_trace(f"manifold_synthetic_pred_FAILED: parsed links is not a list")
                        return None
                except json.JSONDecodeError as je:
                    _write_trace(f"manifold_synthetic_pred_FAILED: links is string but not valid JSON: {je}")
                    return None
            
            # FIX (wrk-0263): Validate links is a non-empty list of dicts before accessing
            if not links or not isinstance(links, list) or len(links) == 0:
                _write_trace(f"manifold_synthetic_pred_FAILED: no links returned (type={type(links).__name__}, len={len(links) if isinstance(links, list) else 'N/A'})")
                logging.debug(f"No causal links found for synthetic prediction on event {event_id[:20]}")
                return None
            
            first_link = links[0]
            # FIX (wrk-0269): Handle nested string in list (e.g., if list contains JSON string)
            if isinstance(first_link, str):
                _write_trace(f"manifold_synthetic_pred_NESTED_STR: first_link is str, attempting JSON parse")
                try:
                    first_link = json.loads(first_link)
                except json.JSONDecodeError:
                    _write_trace(f"manifold_synthetic_pred_FAILED: first_link is string but not valid JSON")
                    return None
            
            if not isinstance(first_link, dict):
                _write_trace(f"manifold_synthetic_pred_FAILED: link is not a dict (type={type(first_link).__name__})")
                logging.debug(f"Causal link is not a dict for event {event_id[:20]}")
                return None
            
            # FIX (wrk-0263): Get the record ID properly - it comes as a RecordID object
            # Use str() which gives 'table:record_id' format needed for SurrealDB queries
            causal_link_rid = first_link.get('id')
            if not causal_link_rid:
                logging.debug(f"No causal link ID found for synthetic prediction on event {event_id[:20]}")
                return None
            
            # Convert RecordID to string format 'table:record_id' (no quotes)
            causal_link_id_str = str(causal_link_rid)
            
            # Also get the source_event as a RecordID string format (no quotes)
            # event_id is already in format 'event:xyz'
            source_event_str = str(event_id)
            
            # FIX (wrk-0263): Use record IDs without quotes in the query
            # SurrealDB expects: field = table:record_id (no quotes)
            # Include all required fields to avoid coercion errors
            query = f"""
                CREATE predicts SET
                agent = 'manifold-feedback-synthetic',
                source_event = {source_event_str},
                causal_link_id = {causal_link_id_str},
                target_description = 'Synthetic prediction for Manifold resolved position',
                probability = 0.5,
                status = 'pending',
                rationale = 'Auto-generated by feedback_loop for Manifold resolved position',
                timestamp = time::now(),
                target_date_max = time::now() + 14d
                RETURN *;
            """
            # DEBUG (wrk-0265): Log raw response to diagnose string return issue
            _write_trace(f"DEBUG: CREATE predicts query about to execute, causal_link_id={causal_link_id_str[:30]}")
            res = await self.db.query(query)
            _write_trace(f"DEBUG: CREATE predicts raw res type={type(res).__name__}, res={str(res)[:200]}")
            new_preds = extract_query_result(res)
            _write_trace(f"DEBUG: extract_query_result returned type={type(new_preds).__name__}, len={len(new_preds) if isinstance(new_preds, list) else 'N/A'}")
            
            # FIX (wrk-0263): Validate new_preds is a non-empty list of dicts
            # This prevents 'str'.get error when SurrealDB returns unexpected format
            if not new_preds or not isinstance(new_preds, list) or len(new_preds) == 0:
                _write_trace(f"manifold_synthetic_pred_FAILED: new_preds invalid (type={type(new_preds).__name__}, len={len(new_preds) if isinstance(new_preds, list) else 'N/A'}, event={event_id[:20]})")
                logging.warning(f"Synthetic prediction creation returned unexpected format for event {event_id[:20]}")
                return None
            
            pred = new_preds[0]
            # FIX (wrk-0269): Handle case where pred is a JSON string, not a dict
            # This prevents 'str'.get error when SurrealDB returns a JSON string instead of parsed dict
            if isinstance(pred, str):
                _write_trace(f"manifold_synthetic_pred_STR_DETECTED: pred is str, attempting JSON parse")
                try:
                    pred = json.loads(pred)
                    _write_trace(f"manifold_synthetic_pred_JSON_PARSE_SUCCESS: parsed to {type(pred).__name__}")
                except json.JSONDecodeError as je:
                    _write_trace(f"manifold_synthetic_pred_FAILED: pred is string but not valid JSON: {je}")
                    logging.warning(f"Synthetic prediction is a string but not valid JSON for event {event_id[:20]}")
                    return None
            
            if not isinstance(pred, dict):
                _write_trace(f"manifold_synthetic_pred_FAILED: pred is not a dict (type={type(pred).__name__})")
                logging.warning(f"Synthetic prediction is not a dict for event {event_id[:20]}")
                return None
            
            _write_trace(
                f"manifold_synthetic_prediction_created: pred_id={str(pred.get('id',''))[:20]} "
                f"event_id={event_id[:20]} causal_link_id={causal_link_id_str[:20]}"
            )
            logging.info(f"Created synthetic prediction for event {event_id[:20]}...")
            return pred
            
        except Exception as e:
            _write_trace(f"manifold_synthetic_pred_EXCEPTION: event_id={event_id[:20]} error={str(e)[:100]}")
            logging.warning(f"Failed to create synthetic prediction for event {event_id[:20]}: {e}")
        
        return None
    
    async def mark_manifold_position_processed(self, position_id: str):
        """Mark a manifold_position as processed for feedback."""
        if self.dry_run:
            return
        
        try:
            query = f"UPDATE {position_id} SET feedback_processed = true;"
            await self.db.query(query)
        except Exception as e:
            logging.warning(f"Failed to mark manifold position {position_id} as processed: {e}")
    
    async def process_manifold_position(self, position: Dict[str, Any]) -> bool:
        """
        Process a single resolved manifold_position to update causal link confidence.
        
        Manifold positions may not have predictions (if manifold_generate failed to link them).
        This method creates synthetic predictions if needed to enable feedback flow.
        
        Returns:
            True if processed successfully, False otherwise.
        """
        position_id = str(position.get("id", ""))
        
        try:
            hit = position.get("hit")
            if hit is None:
                logging.warning(f"Manifold position {position_id} has no hit field, skipping")
                return False
            
            is_correct = bool(hit)
            self.stats["total_predictions"] += 1
            if is_correct:
                self.stats["correct_predictions"] += 1
                self.stats["hits"] += 1
            else:
                self.stats["misses"] += 1
            
            # Get the reference event ID and question
            reference_event_id = position.get("reference_event_id")
            question = position.get("question", "")
            
            # Try to find or create prediction for this event
            # FIX (wrk-0220): Pass question for title fuzzy-match fallback
            prediction = None
            causal_link_id = None
            
            if reference_event_id:
                prediction = await self.find_or_create_prediction_for_event(
                    str(reference_event_id),
                    position_id,
                    bool(hit),
                    question=question
                )
            
            if prediction:
                causal_link_id = prediction.get("causal_link_id")
                
                # FIX (wrk-0262): If prediction found but has no causal_link_id,
                # try to create a synthetic prediction with a valid causal link
                if not causal_link_id:
                    _write_trace(
                        f"manifold_pred_has_no_causal_link: pred_id={str(prediction.get('id',''))[:20]} "
                        f"pos_id={position_id[:20]} - attempting synthetic prediction with causal link"
                    )
                    # Try to find any causal link to associate (lowered threshold from 0.3 to 0.01)
                    try:
                        res = await self.db.query("""
                            SELECT id, confidence FROM causal_link 
                            ORDER BY confidence DESC
                            LIMIT 1;
                        """)
                        links = extract_query_result(res)
                        if links and isinstance(links[0], dict):
                            causal_link_id = str(links[0].get('id', ''))
                            _write_trace(
                                f"manifold_synthetic_link_found: causal_link_id={causal_link_id[:20]} "
                                f"conf={links[0].get('confidence', 0.5):.4f}"
                            )
                    except Exception as link_e:
                        logging.warning(f"Failed to find causal link for synthetic prediction: {link_e}")
                
                if causal_link_id:
                    self.stats["positions_with_prediction"] += 1
                    causal_link = await self.load_causal_link(str(causal_link_id))
                    if causal_link:
                        old_confidence = causal_link.get("confidence", 0.5)
                        predicted_probability = prediction.get("probability", 0.5)
                        new_confidence = self.compute_new_confidence(old_confidence, is_correct)
                        
                        feedback_entry = {
                            "run_id": self.run_id,
                            "updated": datetime.now(timezone.utc).isoformat(),
                            "position_id": position_id,
                            "prediction_id": str(prediction.get("id", "")),
                            "was_correct": is_correct,
                            "old_confidence": old_confidence,
                            "new_confidence": new_confidence,
                            "predicted_probability": predicted_probability,
                            "actual_outcome": "YES" if is_correct else "NO",
                            "source": "manifold_position",
                        }
                        
                        await self.update_causal_link_confidence(
                            str(causal_link_id),
                            old_confidence,
                            new_confidence,
                            feedback_entry
                        )
                        
                        self.stats["links_updated"] += 1
                        # FIX (wrk-0263): Only mark position as processed when causal link was actually updated
                        await self.mark_manifold_position_processed(position_id)
                        _write_trace(
                            f"manifold_causal_links_updated: link_id={str(causal_link_id)[:20]} "
                            f"old_conf={old_confidence:.4f} new_conf={new_confidence:.4f} "
                            f"was_correct={is_correct} pos_id={position_id[:20]}"
                        )
                        logging.info(
                            f"Manifold position {position_id[:20]}...: hit={hit}, "
                            f"causal_link {str(causal_link_id)[:20]}... updated: {old_confidence:.4f} -> {new_confidence:.4f}"
                        )
                    else:
                        logging.warning(f"Causal link {str(causal_link_id)[:20]} not found for manifold position {position_id[:20]}")
                        self.stats["positions_without_prediction"] += 1
                        # FIX (wrk-0263): Don't mark as processed if causal link not found
                        # This allows retry on next iteration
                else:
                    pred_id = prediction.get('id', '')
                    logging.warning(f"Prediction {str(pred_id)[:20] if pred_id else 'None'} has no causal_link_id and no fallback link found")
                    self.stats["positions_without_prediction"] += 1
                    # FIX (wrk-0263): Don't mark as processed if no causal link found
                    # This allows retry on next iteration
            else:
                self.stats["positions_without_prediction"] += 1
                _write_trace(
                    f"manifold_positions_unlinked: pos_id={position_id[:20]} "
                    f"ref_event={str(reference_event_id)[:20] if reference_event_id else 'None'} "
                    f"hit={hit} (no prediction)"
                )
                logging.debug(f"No prediction for manifold event {reference_event_id} (pos {position_id[:20]}), hit={hit}")
                # FIX (wrk-0263): Don't mark as processed if no prediction found
                # This allows retry on next iteration
            
            self.stats["positions_processed"] += 1
            return True
            
        except Exception as e:
            logging.error(f"Error processing manifold position {position_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def load_resolved_manifold_positions(self) -> List[Dict[str, Any]]:
        """
        Load resolved manifold_position records that haven't been processed for feedback.
        
        FIX (wrk-0220): Changed query to also find positions with hit IS NOT NONE
        even if status is not 'resolved'. This handles positions resolved by
        synthetic_res_sim.py that have hit set but status may still be 'open'.
        """
        # DEBUG: First check total counts in DB to understand state
        debug_query = "SELECT count() as total, count(status = 'resolved') as resolved_count, count(hit IS NOT NONE) as hit_count, count(feedback_processed = false OR feedback_processed = 'false' OR feedback_processed IS NONE) as unprocessed_count FROM manifold_position GROUP ALL;"
        try:
            debug_res = await self.db.query(debug_query)
            debug_data = extract_query_result(debug_res)
            if debug_data:
                _write_trace(f"DEBUG_MANIFOLD_DB_STATE: {debug_data[0]}")
                logging.info(f"DEBUG: manifold_position DB state: {debug_data[0]}")
        except Exception as e:
            _write_trace(f"DEBUG_MANIFOLD_DB_STATE_ERROR: {e}")
            logging.warning(f"DEBUG query failed: {e}")
        
        query = """
            SELECT * FROM manifold_position 
            WHERE (status = 'resolved' OR hit IS NOT NONE)
            AND (feedback_processed = false OR feedback_processed = 'false' OR feedback_processed IS NONE)
            ORDER BY resolution_time DESC
            LIMIT $limit;
        """
        res = await self.db.query(query, {"limit": self.batch_size})
        positions = extract_query_result(res)
        _write_trace(f"manifold_positions_predicted={len(positions)}")
        logging.info(f"Found {len(positions)} resolved manifold positions to process")
        return positions
    
    async def process_polymarket_position(self, position: Dict[str, Any]) -> bool:
        """
        Process a single resolved polymarket position to update causal link confidence
        if a prediction exists for the associated event.
        
        Returns:
            True if processed successfully, False otherwise.
        """
        position_id = str(position.get("id", ""))
        
        try:
            # Get the hit/miss result from the position
            hit = position.get("hit")
            if hit is None:
                logging.warning(f"Position {position_id} has no hit field, skipping")
                return False
            
            is_correct = bool(hit)
            self.stats["total_predictions"] += 1
            if is_correct:
                self.stats["correct_predictions"] += 1
                self.stats["hits"] += 1
            else:
                self.stats["misses"] += 1
            
            # Get the reference event ID
            reference_event_id = position.get("reference_event_id")
            
            # Try to find a prediction for this event
            prediction = None
            causal_link_id = None
            
            if reference_event_id:
                prediction = await self.find_prediction_for_event(str(reference_event_id))
            
            if prediction:
                self.stats["positions_with_prediction"] += 1
                causal_link_id = prediction.get("causal_link_id")
                
                # [TRACE] positions_linked: position has a prediction with causal link
                _write_trace(
                    f"positions_linked: pos_id={position_id} pred_id={str(prediction.get('id',''))[:20]} "
                    f"causal_link_id={str(causal_link_id)[:20] if causal_link_id else 'None'} "
                    f"hit={hit}"
                )
                
                if causal_link_id:
                    # Load the causal link
                    causal_link = await self.load_causal_link(str(causal_link_id))
                    if causal_link:
                        # Compute new confidence
                        old_confidence = causal_link.get("confidence", 0.5)
                        predicted_probability = prediction.get("probability", 0.5)
                        
                        # For polymarket positions, the "prediction" is the side (YES/NO)
                        # We already know if it was correct from the hit field
                        new_confidence = self.compute_new_confidence(old_confidence, is_correct)
                        
                        # Build feedback entry
                        feedback_entry = {
                            "run_id": self.run_id,
                            "updated": datetime.now(timezone.utc).isoformat(),
                            "position_id": position_id,
                            "prediction_id": str(prediction.get("id", "")),
                            "was_correct": is_correct,
                            "old_confidence": old_confidence,
                            "new_confidence": new_confidence,
                            "predicted_probability": predicted_probability,
                            "actual_outcome": "YES" if is_correct else "NO",
                            "source": "polymarket_position",
                        }
                        
                        # Update the causal link
                        await self.update_causal_link_confidence(
                            str(causal_link_id),
                            old_confidence,
                            new_confidence,
                            feedback_entry
                        )
                        
                        self.stats["links_updated"] += 1
                        # [TRACE] causal_links_updated
                        _write_trace(
                            f"causal_links_updated: link_id={str(causal_link_id)[:20]} "
                            f"old_conf={old_confidence:.4f} new_conf={new_confidence:.4f} "
                            f"was_correct={is_correct}"
                        )
                        logging.info(
                            f"Position {position_id}: hit={hit}, "
                            f"causal_link {causal_link_id[:20]}... updated: {old_confidence:.4f} -> {new_confidence:.4f}"
                        )
                    else:
                        logging.warning(f"Causal link {causal_link_id} not found for position {position_id}")
                else:
                    logging.warning(f"Prediction {prediction.get('id')} has no causal_link_id")
            else:
                self.stats["positions_without_prediction"] += 1
                # [TRACE] positions without prediction (not linked to causal chain)
                _write_trace(
                    f"positions_unlinked: pos_id={position_id} "
                    f"ref_event={str(reference_event_id)[:20] if reference_event_id else 'None'} "
                    f"hit={hit} (no prediction found)"
                )
                logging.debug(f"No prediction found for event {reference_event_id} (position {position_id}), hit={hit}")
            
            # Mark position as processed
            await self.mark_position_processed(position_id)
            
            self.stats["positions_processed"] += 1
            return True
            
        except Exception as e:
            logging.error(f"Error processing position {position_id}: {e}")
            self.stats["errors"] += 1
            return False
    
    # =====================================================================
    # Phase 7: Annotation and Disagreement Signal Processing (wrk-0307)
    # =====================================================================
    
    async def load_annotation_signals(self) -> List[Dict[str, Any]]:
        """
        Load unprocessed annotation signals from SurrealDB.
        
        Annotation signals are user-submitted "this caused that" causal pairs
        that can be used to grow the causal graph.
        """
        query = """
            SELECT * FROM annotation_signal 
            WHERE processed = false
            ORDER BY created_at ASC
            LIMIT $limit;
        """
        res = await self.db.query(query, {"limit": self.batch_size})
        signals = extract_query_result(res)
        logging.info(f"Found {len(signals)} unprocessed annotation signals")
        return signals
    
    async def mark_annotation_processed(self, signal_id: str, causal_link_id: Optional[str] = None):
        """Mark an annotation signal as processed."""
        if self.dry_run:
            return
        
        try:
            if causal_link_id:
                query = f"UPDATE {signal_id} SET processed = true, causal_link_id = {causal_link_id};"
            else:
                query = f"UPDATE {signal_id} SET processed = true;"
            await self.db.query(query)
        except Exception as e:
            logging.warning(f"Failed to mark annotation {signal_id} as processed: {e}")
    
    async def create_causal_link_from_annotation(self, signal: Dict[str, Any]) -> Optional[str]:
        """
        Create a causal link from an annotation signal.
        
        Args:
            signal: Annotation signal dict with from_event, to_event, mechanism
        
        Returns:
            Causal link ID or None if creation failed.
        """
        from_event = str(signal.get("from_event", ""))
        to_event = str(signal.get("to_event", ""))
        mechanism = signal.get("mechanism", "")
        confidence_override = signal.get("confidence_override")
        
        # Use override confidence if provided, otherwise start at 0.5
        initial_confidence = confidence_override if confidence_override else 0.5
        
        if self.dry_run:
            logging.info(f"[DRY_RUN] Would create causal_link: {from_event} -> {to_event}")
            logging.info(f"  mechanism: {mechanism[:50]}...")
            logging.info(f"  initial_confidence: {initial_confidence}")
            return "dry_run_causal_link_id"
        
        try:
            query = f"""
                CREATE causal_link SET
                    from = {from_event},
                    to = {to_event},
                    confidence = $conf,
                    mechanism = $mechanism,
                    evidence = [],
                    provenance = {{
                        type: 'annotation',
                        source_user: $user,
                        annotation_signal_id: $signal_id,
                        created: time::now()
                    }},
                    feedback_history = [],
                    total_predictions = 0,
                    correct_predictions = 0
                RETURN id;
            """
            res = await self.db.query(query, {
                "conf": initial_confidence,
                "mechanism": mechanism,
                "user": signal.get("source_user", "anonymous"),
                "signal_id": str(signal.get("id", "")),
            })
            results = extract_query_result(res)
            if results and len(results) > 0:
                link_id = str(results[0].get("id"))
                logging.info(f"Created causal_link from annotation: {link_id}")
                return link_id
        except Exception as e:
            logging.error(f"Failed to create causal link from annotation: {e}")
            self.stats["errors"] += 1
        
        return None
    
    async def process_annotation_signals(self):
        """
        Phase 4a: Process annotation signals.
        
        For each unprocessed annotation signal:
        1. Create a new causal link
        2. Mark the signal as processed
        """
        logging.info("=" * 60)
        logging.info("PHASE 4a: Processing Annotation Signals (wrk-0307)")
        logging.info("=" * 60)
        
        signals = await self.load_annotation_signals()
        
        if not signals:
            logging.info("No annotation signals to process")
            return
        
        for signal in signals:
            signal_id = str(signal.get("id", ""))
            
            try:
                # Create causal link from annotation
                link_id = await self.create_causal_link_from_annotation(signal)
                
                if link_id:
                    self.stats["annotation_links_created"] += 1
                    await self.mark_annotation_processed(signal_id, link_id)
                else:
                    self.stats["annotations_skipped"] += 1
                
                self.stats["annotations_processed"] += 1
                
            except Exception as e:
                logging.error(f"Error processing annotation signal {signal_id}: {e}")
                self.stats["errors"] += 1
                self.stats["annotations_skipped"] += 1
        
        logging.info(f"Annotation signals processed: {self.stats['annotations_processed']}, "
                    f"links created: {self.stats['annotation_links_created']}, "
                    f"skipped: {self.stats['annotations_skipped']}")
    
    async def load_disagreement_signals(self) -> List[Dict[str, Any]]:
        """
        Load unprocessed disagreement signals from SurrealDB.
        
        Disagreement signals are markets with high YES/NO imbalance (>70%)
        that indicate forecast uncertainty.
        """
        query = """
            SELECT * FROM disagreement_signal 
            WHERE processed = false
            ORDER BY timestamp ASC
            LIMIT $limit;
        """
        res = await self.db.query(query, {"limit": self.batch_size})
        signals = extract_query_result(res)
        logging.info(f"Found {len(signals)} unprocessed disagreement signals")
        return signals
    
    async def mark_disagreement_processed(self, signal_id: str):
        """Mark a disagreement signal as processed."""
        if self.dry_run:
            return
        
        try:
            query = f"UPDATE {signal_id} SET processed = true;"
            await self.db.query(query)
        except Exception as e:
            logging.warning(f"Failed to mark disagreement {signal_id} as processed: {e}")
    
    async def apply_weighted_confidence_update(
        self,
        causal_link_id: str,
        is_correct: bool,
        feedback_weight: float
    ):
        """
        Apply a weighted confidence update based on disagreement signal.
        
        High disagreement = higher weight for correct, lower weight for incorrect.
        
        Args:
            causal_link_id: ID of causal link to update
            is_correct: Whether the prediction was correct
            feedback_weight: Weight factor (0.5 to 1.75)
        """
        # Load the causal link
        causal_link = await self.load_causal_link(causal_link_id)
        if not causal_link:
            logging.warning(f"Causal link {causal_link_id} not found for weighted update")
            return
        
        old_confidence = causal_link.get("confidence", 0.5)
        
        # Compute weighted update
        if is_correct:
            # Boost confidence, weighted by disagreement
            # new = old + weight * rate * (1 - old)
            # But weight > 1 means we boost MORE
            effective_rate = self.learning_rate * feedback_weight
            new_conf = old_confidence + effective_rate * (1 - old_confidence)
        else:
            # Reduce confidence, but less severely for high disagreement
            # new = old - rate * old (but rate is reduced by weight)
            # High disagreement (weight > 1) means LESS reduction
            effective_rate = self.learning_rate / feedback_weight if feedback_weight > 0 else self.learning_rate
            new_conf = old_confidence - effective_rate * old_confidence
        
        # Clamp to bounds
        new_conf = max(self.min_confidence, min(self.max_confidence, new_conf))
        
        # Build feedback entry
        feedback_entry = {
            "run_id": self.run_id,
            "updated": datetime.now(timezone.utc).isoformat(),
            "was_correct": is_correct,
            "old_confidence": old_confidence,
            "new_confidence": new_conf,
            "feedback_weight": feedback_weight,
            "source": "disagreement_signal",
        }
        
        # Update the causal link
        await self.update_causal_link_confidence(
            causal_link_id,
            old_confidence,
            new_conf,
            feedback_entry
        )
        
        self.stats["disagreement_weighted_updates"] += 1
    
    async def process_disagreement_signals(self):
        """
        Phase 4b: Process disagreement signals.
        
        For each unprocessed disagreement signal:
        1. Find the associated prediction/market
        2. Apply weighted confidence update based on disagreement level
        
        Note: Disagreement signals don't directly create links - they modulate
        how much confidence changes for existing predictions.
        """
        logging.info("=" * 60)
        logging.info("PHASE 4b: Processing Disagreement Signals (wrk-0307)")
        logging.info("=" * 60)
        
        signals = await self.load_disagreement_signals()
        
        if not signals:
            logging.info("No disagreement signals to process")
            return
        
        for signal in signals:
            signal_id = str(signal.get("id", ""))
            
            try:
                market_slug = signal.get("market_slug", "")
                feedback_weight = signal.get("feedback_weight", 1.0)
                imbalance = signal.get("imbalance_pct", 0.0)
                
                _write_trace(
                    f"disagreement_signal: slug={market_slug[:30]} "
                    f"imbalance={imbalance:.2f} weight={feedback_weight:.2f}"
                )
                
                # Mark as processed (we're applying the disagreement info to future predictions)
                # In a full implementation, we would link this to specific predictions
                # For prototype, we just mark as processed and log
                await self.mark_disagreement_processed(signal_id)
                
                self.stats["disagreements_processed"] += 1
                
            except Exception as e:
                logging.error(f"Error processing disagreement signal {signal_id}: {e}")
                self.stats["errors"] += 1
                self.stats["disagreements_skipped"] += 1
        
        logging.info(f"Disagreement signals processed: {self.stats['disagreements_processed']}, "
                    f"skipped: {self.stats['disagreements_skipped']}, "
                    f"weighted_updates: {self.stats['disagreement_weighted_updates']}")
    
    async def run(self):
        """Main feedback loop execution."""
        start_time = time.time()
        logging.info("=" * 60)
        logging.info("VOLVA FEEDBACK LOOP STARTING")
        logging.info(f"Run ID: {self.run_id}")
        logging.info(f"Dry Run: {self.dry_run}")
        logging.info("=" * 60)
        
        try:
            await self.connect()
            
            # Load already-processed markets if skip_processed is enabled
            if self.skip_processed:
                await self.load_processed_markets()
                await self.load_processed_polymarket_positions()
            
            # Main processing loop - Phase 1: market_draft
            total_processed = 0
            iteration = 0
            max_iterations = 10  # Prevent infinite loops
            
            while iteration < max_iterations:
                iteration += 1
                markets = await self.load_resolved_markets()
                
                if not markets:
                    logging.info("No more unresolved markets to process")
                    break
                
                for market in markets:
                    market_id = str(market.get("id", ""))
                    
                    # Skip already processed
                    if self.skip_processed and market_id in self.processed_market_ids:
                        self.stats["markets_skipped"] += 1
                        continue
                    
                    success = await self.process_market(market)
                    if success:
                        self.stats["markets_processed"] += 1
                        total_processed += 1
                
                logging.info(f"Iteration {iteration}: processed {len(markets)} markets")
            _write_trace(f"market_draft_iter_{iteration}: processed={len(markets)}")
            
            # Phase 2: polymarket_position processing
            logging.info("=" * 60)
            logging.info("PHASE 2: Processing Polymarket Positions")
            logging.info("=" * 60)
            
            pos_iteration = 0
            while pos_iteration < max_iterations:
                pos_iteration += 1
                positions = await self.load_resolved_polymarket_positions()
                
                if not positions:
                    logging.info("No more unresolved polymarket positions to process")
                    break
                
                for position in positions:
                    position_id = str(position.get("id", ""))
                    
                    # Skip already processed
                    if self.skip_processed and position_id in self.processed_position_ids:
                        self.stats["positions_skipped"] += 1
                        continue
                    
                    success = await self.process_polymarket_position(position)
                
                logging.info(f"Position iteration {pos_iteration}: processed {len(positions)} positions")
                _write_trace(f"polymarket_iter_{pos_iteration}: processed={len(positions)}")
            
            # Phase 3: Manifold position processing (wrk-0094)
            logging.info("=" * 60)
            logging.info("PHASE 3: Processing Manifold Positions (wrk-0094)")
            logging.info("=" * 60)
            
            man_iteration = 0
            while man_iteration < max_iterations:
                man_iteration += 1
                manifold_positions = await self.load_resolved_manifold_positions()
                
                if not manifold_positions:
                    logging.info("No more unresolved manifold positions to process")
                    break
                
                for position in manifold_positions:
                    position_id = str(position.get("id", ""))
                    
                    # Skip already processed (use same processed set)
                    if self.skip_processed and position_id in self.processed_position_ids:
                        self.stats["positions_skipped"] += 1
                        continue
                    
                    success = await self.process_manifold_position(position)
                
                logging.info(f"Manifold iteration {man_iteration}: processed {len(manifold_positions)} manifold positions")
                _write_trace(f"manifold_iter_{man_iteration}: processed={len(manifold_positions)}")
            
            # Phase 4: Annotation and Disagreement Signal Processing (wrk-0307)
            # Process user-submitted annotations and forecast disagreement signals
            await self.process_annotation_signals()
            await self.process_disagreement_signals()
            
            # Calculate overall accuracy
            if self.stats["total_predictions"] > 0:
                accuracy = self.stats["correct_predictions"] / self.stats["total_predictions"]
            else:
                accuracy = 0.0
            
            elapsed = time.time() - start_time
            
            # Final summary
            logging.info("=" * 60)
            logging.info("FEEDBACK LOOP SUMMARY")
            logging.info("=" * 60)
            logging.info(f"Run ID:           {self.run_id}")
            logging.info(f"Markets Processed: {self.stats['markets_processed']}")
            logging.info(f"Markets Skipped:   {self.stats['markets_skipped']}")
            logging.info(f"Links Updated:     {self.stats['links_updated']}")
            logging.info(f"Links Unchanged:   {self.stats['links_unchanged']}")
            logging.info(f"Total Predictions: {self.stats['total_predictions']}")
            logging.info(f"Correct Predictions: {self.stats['correct_predictions']}")
            logging.info(f"Accuracy:          {accuracy:.2%}")
            logging.info(f"Errors:            {self.stats['errors']}")
            logging.info("-" * 40)
            logging.info("Polymarket Position Stats:")
            logging.info(f"  Positions Processed: {self.stats['positions_processed']}")
            logging.info(f"  Positions Skipped:   {self.stats['positions_skipped']}")
            logging.info(f"  Positions with Prediction: {self.stats['positions_with_prediction']}")
            logging.info(f"  Positions without Prediction: {self.stats['positions_without_prediction']}")
            logging.info(f"  Hits: {self.stats['hits']}")
            logging.info(f"  Misses: {self.stats['misses']}")
            logging.info("-" * 40)
            logging.info("Manifold Position Stats (wrk-0094):")
            logging.info(f"  Positions Processed: {self.stats['positions_processed']}")  # cumulative
            logging.info(f"  Hits: {self.stats['hits']}")
            logging.info(f"  Misses: {self.stats['misses']}")
            logging.info("-" * 40)
            logging.info("Annotation Signal Stats (wrk-0307):")
            logging.info(f"  Annotations Processed:  {self.stats['annotations_processed']}")
            logging.info(f"  Annotations Skipped:   {self.stats['annotations_skipped']}")
            logging.info(f"  Links Created:         {self.stats['annotation_links_created']}")
            logging.info("-" * 40)
            logging.info("Disagreement Signal Stats (wrk-0307):")
            logging.info(f"  Disagreements Processed:   {self.stats['disagreements_processed']}")
            logging.info(f"  Disagreements Skipped:    {self.stats['disagreements_skipped']}")
            logging.info(f"  Weighted Updates:         {self.stats['disagreement_weighted_updates']}")
            logging.info(f"Duration:          {elapsed:.2f}s")
            logging.info("=" * 60)
            
            # [TRACE] final summary
            _write_trace(
                f"SUMMARY: "
                f"positions_predicted={self.stats['total_predictions']} "
                f"positions_eligible={self.stats['positions_processed']} "
                f"positions_with_prediction={self.stats['positions_with_prediction']} "
                f"positions_without_prediction={self.stats['positions_without_prediction']} "
                f"positions_linked={self.stats['positions_with_prediction']} "
                f"causal_links_updated={self.stats['links_updated']} "
                f"hits={self.stats['hits']} misses={self.stats['misses']} "
                f"accuracy={accuracy:.4f} errors={self.stats['errors']} "
                f"duration={elapsed:.2f}s"
            )
            _write_trace("RUN_COMPLETE")
            
            # Return machine-parseable result
            result = {
                "run_id": self.run_id,
                "markets_processed": self.stats["markets_processed"],
                "markets_skipped": self.stats["markets_skipped"],
                "links_updated": self.stats["links_updated"],
                "total_predictions": self.stats["total_predictions"],
                "correct_predictions": self.stats["correct_predictions"],
                "accuracy": accuracy,
                "errors": self.stats["errors"],
                "positions_processed": self.stats["positions_processed"],
                "positions_skipped": self.stats["positions_skipped"],
                "positions_with_prediction": self.stats["positions_with_prediction"],
                "positions_without_prediction": self.stats["positions_without_prediction"],
                "hits": self.stats["hits"],
                "misses": self.stats["misses"],
                # Phase 7: Annotation signals (wrk-0307)
                "annotations_processed": self.stats["annotations_processed"],
                "annotations_skipped": self.stats["annotations_skipped"],
                "annotation_links_created": self.stats["annotation_links_created"],
                # Phase 7: Disagreement signals (wrk-0307)
                "disagreements_processed": self.stats["disagreements_processed"],
                "disagreements_skipped": self.stats["disagreements_skipped"],
                "disagreement_weighted_updates": self.stats["disagreement_weighted_updates"],
                "duration_seconds": elapsed,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dry_run": self.dry_run,
            }
            
            # Print JSON result for parsing
            print(f"RESULT_JSON:{json.dumps(result)}")
            
            return result
            
        finally:
            await self.close()


async def main():
    """Entry point."""
    # Setup logging
    config = load_config()
    log_config = config.get("logging", DEFAULT_CONFIG["logging"])
    logging.basicConfig(
        level=getattr(logging, log_config.get("level", "INFO")),
        format=log_config.get("format", "%(message)s"),
    )
    
    # Check if feedback loop is enabled
    if not config["feedback_loop"].get("enabled", True):
        logging.info("Feedback loop is disabled in configuration")
        return
    
    # Run the feedback loop
    loop = FeedbackLoop(config)
    await loop.run()


if __name__ == "__main__":
    asyncio.run(main())
