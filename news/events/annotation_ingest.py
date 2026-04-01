#!/usr/bin/env python3
"""
Volva Annotation Ingest - Phase 7 (wrk-0307)
Prototype for ingesting user-submitted "this caused that" causal annotations.

This script reads annotations from a JSON file and stores them in SurrealDB
for processing by the feedback loop.

Usage:
    PYTHONUNBUFFERED=1 python3 annotation_ingest.py [--dry-run]

Input Format (annotation_signals.json):
    [
        {
            "from_event_content": "Description of source event",
            "to_event_content": "Description of outcome event",
            "mechanism": "How A caused B",
            "source_user": "user@example.com",
            "confidence_override": 0.85  // optional
        },
        ...
    ]

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

from surrealdb import AsyncSurreal

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Database settings (from predictor_config)
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("volva.annotation_ingest")


def extract_query_result(res):
    """Handle SurrealDB 1.x response envelopes and direct results."""
    if not res:
        return []
    if isinstance(res, list) and res and isinstance(res[0], dict) and "result" in res[0]:
        return res[0]["result"] if res[0]["result"] else []
    return res if isinstance(res, list) else [res]


class AnnotationIngest:
    """
    Ingest user-submitted causal annotations into SurrealDB.
    
    Annotations are "this caused that" signals from users that can be
    used to grow the causal graph when they align with prediction outcomes.
    """
    
    def __init__(self, dry_run: bool = False):
        self.db: Optional[AsyncSurreal] = None
        self.dry_run = dry_run
        self.run_id = str(uuid.uuid4())[:8]
        self.stats = {
            "annotations_loaded": 0,
            "events_found": 0,
            "events_created": 0,
            "signals_stored": 0,
            "signals_skipped_dup": 0,
            "errors": 0,
        }
        self.dedup_keys: set = set()
        
        logger.info(f"AnnotationIngest initialized (run_id={self.run_id}, dry_run={self.dry_run})")
    
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
    
    async def find_or_create_event(self, content: str) -> Optional[str]:
        """
        Find existing event by content or create a new one.
        
        Returns:
            Event ID string or None if creation failed.
        """
        if not content:
            return None
        
        # Normalize content for comparison
        normalized = content.strip().lower()[:500]  # First 500 chars
        
        # Check if event already exists (by content match)
        query = """
            SELECT id, content FROM event 
            WHERE content CONTAINS $content
            LIMIT 1;
        """
        res = await self.db.query(query, {"content": normalized[:100]})
        events = extract_query_result(res)
        
        if events and len(events) > 0:
            event = events[0]
            logger.debug(f"Found existing event: {event.get('id')} - {event.get('content')[:50]}...")
            return str(event.get("id"))
        
        # Create new event
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would create event: {content[:50]}...")
            return None
        
        try:
            create_query = """
                CREATE event SET
                    content = $content,
                    timestamp = time::now(),
                    verified = false,
                    metadata = {
                        source: 'annotation_ingest',
                        run_id: $run_id,
                        created_at: time::now()
                    }
                RETURN id;
            """
            res = await self.db.query(create_query, {
                "content": content,
                "run_id": self.run_id
            })
            results = extract_query_result(res)
            if results and len(results) > 0:
                event_id = str(results[0].get("id"))
                logger.debug(f"Created new event: {event_id}")
                return event_id
        except Exception as e:
            logger.error(f"Failed to create event: {e}")
        
        return None
    
    def generate_dedup_key(self, from_content: str, to_content: str) -> str:
        """Generate a deduplication key for an annotation pair."""
        from_norm = from_content.strip().lower()[:100]
        to_norm = to_content.strip().lower()[:100]
        return f"{from_norm}|{to_norm}"
    
    async def check_annotation_exists(self, from_event_id: str, to_event_id: str) -> bool:
        """Check if an annotation signal already exists for this pair."""
        query = """
            SELECT id FROM annotation_signal 
            WHERE from_event = $from_id 
            AND to_event = $to_id
            LIMIT 1;
        """
        res = await self.db.query(query, {"from_id": from_event_id, "to_id": to_event_id})
        signals = extract_query_result(res)
        return len(signals) > 0
    
    async def store_annotation_signal(
        self,
        from_event_id: str,
        to_event_id: str,
        mechanism: str,
        source_user: str,
        confidence_override: Optional[float] = None,
        from_content: str = "",
        to_content: str = ""
    ) -> Optional[str]:
        """
        Store an annotation signal in SurrealDB.
        
        Returns:
            annotation_signal ID or None if failed.
        """
        # Check for duplicate
        dedup_key = self.generate_dedup_key(from_content, to_content)
        if dedup_key in self.dedup_keys:
            logger.debug(f"Skipping duplicate annotation: {dedup_key[:50]}...")
            self.stats["signals_skipped_dup"] += 1
            return None
        
        self.dedup_keys.add(dedup_key)
        
        # Check DB for existing
        if await self.check_annotation_exists(from_event_id, to_event_id):
            logger.debug(f"Annotation signal already exists in DB for {from_event_id} -> {to_event_id}")
            self.stats["signals_skipped_dup"] += 1
            return None
        
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would store annotation_signal: {from_event_id} -> {to_event_id}")
            logger.info(f"  mechanism: {mechanism[:50]}...")
            logger.info(f"  source_user: {source_user}")
            return None
        
        try:
            # FIX (wrk-0309): Use f-string interpolation for record<> fields
            # SurrealDB expects record<> fields as unquoted RecordID literals (e.g., event:xyz)
            # not as quoted string parameters which cause coercion errors
            query = f"""
                CREATE annotation_signal SET
                    from_event = {from_event_id},
                    to_event = {to_event_id},
                    mechanism = $mechanism,
                    source_user = $source_user,
                    confidence_override = $conf_override,
                    created_at = time::now(),
                    processed = false,
                    metadata = $metadata
                RETURN id;
            """
            res = await self.db.query(query, {
                "mechanism": mechanism,
                "source_user": source_user,
                "conf_override": confidence_override,
                "run_id": self.run_id,
                "metadata": {
                    "run_id": self.run_id,
                    "from_content": from_content[:500] if from_content else "",
                    "to_content": to_content[:500] if to_content else "",
                },
            })
            results = extract_query_result(res)
            if results and len(results) > 0:
                signal_id = str(results[0].get("id"))
                logger.info(f"Stored annotation_signal: {signal_id}")
                self.stats["signals_stored"] += 1
                return signal_id
        except Exception as e:
            logger.error(f"Failed to store annotation_signal: {e}")
            self.stats["errors"] += 1
        
        return None
    
    async def process_annotation(self, annotation: Dict[str, Any]) -> bool:
        """
        Process a single annotation entry.
        
        Args:
            annotation: Dict with keys: from_event_content, to_event_content, 
                       mechanism, source_user, confidence_override (optional)
        
        Returns:
            True if processed successfully, False otherwise.
        """
        try:
            from_content = annotation.get("from_event_content", "")
            to_content = annotation.get("to_event_content", "")
            mechanism = annotation.get("mechanism", "")
            source_user = annotation.get("source_user", "anonymous")
            confidence_override = annotation.get("confidence_override")
            
            if not from_content or not to_content:
                logger.warning(f"Skipping annotation missing from/to content")
                return False
            
            if not mechanism:
                logger.warning(f"Skipping annotation missing mechanism")
                return False
            
            self.stats["annotations_loaded"] += 1
            
            # Find or create events
            from_event_id = await self.find_or_create_event(from_content)
            to_event_id = await self.find_or_create_event(to_content)
            
            if from_event_id:
                self.stats["events_found"] += 1
            else:
                self.stats["events_created"] += 1
            
            if to_event_id:
                self.stats["events_found"] += 1
            else:
                self.stats["events_created"] += 1
            
            if not from_event_id or not to_event_id:
                logger.warning(f"Failed to get event IDs for annotation")
                return False
            
            # Store annotation signal
            signal_id = await self.store_annotation_signal(
                from_event_id=from_event_id,
                to_event_id=to_event_id,
                mechanism=mechanism,
                source_user=source_user,
                confidence_override=confidence_override,
                from_content=from_content,
                to_content=to_content
            )
            
            return signal_id is not None
            
        except Exception as e:
            logger.error(f"Error processing annotation: {e}")
            self.stats["errors"] += 1
            return False
    
    async def load_annotations_from_file(self, filepath: str) -> List[Dict[str, Any]]:
        """Load annotations from JSON file."""
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "annotations" in data:
                    return data["annotations"]
                else:
                    logger.warning(f"Unexpected JSON format in {filepath}")
                    return []
        except FileNotFoundError:
            logger.error(f"Annotation file not found: {filepath}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {filepath}: {e}")
            return []
    
    async def run(self, annotations_file: str = "annotation_signals.json"):
        """Main execution."""
        start_time = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("VOLVA ANNOTATION INGEST STARTING")
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Dry Run: {self.dry_run}")
        logger.info(f"Input File: {annotations_file}")
        logger.info("=" * 60)
        
        try:
            await self.connect()
            
            # Load annotations from file
            annotations = await self.load_annotations_from_file(annotations_file)
            logger.info(f"Loaded {len(annotations)} annotations from {annotations_file}")
            
            if not annotations:
                logger.warning("No annotations to process")
                return self.stats
            
            # Process each annotation
            for annotation in annotations:
                await self.process_annotation(annotation)
            
            # Print summary
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("ANNOTATION INGEST SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Annotations Loaded:  {self.stats['annotations_loaded']}")
            logger.info(f"Events Found:        {self.stats['events_found']}")
            logger.info(f"Events Created:       {self.stats['events_created']}")
            logger.info(f"Signals Stored:      {self.stats['signals_stored']}")
            logger.info(f"Signals Skipped:     {self.stats['signals_skipped_dup']}")
            logger.info(f"Errors:              {self.stats['errors']}")
            logger.info(f"Duration:            {elapsed:.2f}s")
            logger.info("=" * 60)
            
            # Return machine-parseable result
            result = {
                "run_id": self.run_id,
                "annotations_loaded": self.stats["annotations_loaded"],
                "events_found": self.stats["events_found"],
                "events_created": self.stats["events_created"],
                "signals_stored": self.stats["signals_stored"],
                "signals_skipped_dup": self.stats["signals_skipped_dup"],
                "errors": self.stats["errors"],
                "duration_seconds": elapsed,
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
    
    # Get input file from args
    annotations_file = "annotation_signals.json"
    if len(sys.argv) > 1:
        annotations_file = sys.argv[1]
    
    # Make path absolute relative to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    annotations_file = os.path.join(script_dir, annotations_file)
    
    # Run ingest
    ingest = AnnotationIngest(dry_run=dry_run)
    await ingest.run(annotations_file)


if __name__ == "__main__":
    asyncio.run(main())
