#!/usr/bin/env python3
"""
Volva Chain Discoverer - Phase 3
Discovers transitive causal chains (A→B→C) from existing direct causal links.
Uses the transitive probability formula: P(C|A) = Σ P(C|B) * P(B|A) for all paths.

Usage:
    DRY_RUN=true python3 chain_discoverer.py

Environment Variables:
    DRY_RUN: Set to 'true' for dry run mode (no DB writes)
    MAX_CHAIN_LENGTH: Maximum chain length to discover (default: 5)
    MIN_CHAIN_PROBABILITY: Minimum probability threshold (default: 0.5)
"""

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from surrealdb import AsyncSurreal

# Add parent directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import config
try:
    from chain_discoverer_config import (
        SURREALDB_HOST,
        SURREALDB_USER,
        SURREALDB_PASS,
        SURREALDB_NS,
        SURREALDB_DB,
        MAX_CHAIN_LENGTH as CONFIG_MAX_CHAIN_LENGTH,
        MIN_CHAIN_PROBABILITY as CONFIG_MIN_CHAIN_PROBABILITY,
        DRY_RUN as CONFIG_DRY_RUN,
        VERBOSE,
    )
except ImportError:
    # Fallback defaults
    SURREALDB_HOST = "http://127.0.0.1:8001"
    SURREALDB_USER = "root"
    SURREALDB_PASS = "root"
    SURREALDB_NS = "volva"
    SURREALDB_DB = "causal_graph"
    CONFIG_MAX_CHAIN_LENGTH = 5
    CONFIG_MIN_CHAIN_PROBABILITY = 0.5
    CONFIG_DRY_RUN = True
    VERBOSE = True

# Environment variable overrides
DRY_RUN = os.environ.get("DRY_RUN", str(CONFIG_DRY_RUN)).lower() == "true"
MAX_CHAIN_LENGTH = int(os.environ.get("MAX_CHAIN_LENGTH", CONFIG_MAX_CHAIN_LENGTH))
MIN_CHAIN_PROBABILITY = float(os.environ.get("MIN_CHAIN_PROBABILITY", CONFIG_MIN_CHAIN_PROBABILITY))


class ChainDiscoverer:
    """Discovers transitive causal chains from direct causal links."""

    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        self.stats = {
            "links_loaded": 0,
            "chains_discovered": 0,
            "chains_accepted": 0,
            "chains_rejected": 0,
            "chains_duplicated": 0,
            "db_writes": 0,
            "errors": [],
        }
        self.accepted_chains: list = []
        self.seen_chain_keys: set = set()  # For deduplication within run
        self.graph: dict = defaultdict(list)  # from_event -> [(to_event, link_id, confidence), ...]
        self.reverse_graph: dict = defaultdict(list)  # to_event -> [(from_event, link_id, confidence), ...]
        self.link_data: dict = {}  # link_id -> {from, to, confidence, mechanism}

    async def connect(self):
        """Connect to SurrealDB with logging."""
        print(f"[*] Connecting to SurrealDB at {SURREALDB_HOST}...")
        try:
            self.db = AsyncSurreal(SURREALDB_HOST)
            await self.db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
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

    async def verify_pre_state(self) -> dict:
        """Verify DB state before run."""
        print("[*] Verifying pre-run DB state...")
        query = "SELECT count() FROM causal_chain GROUP ALL;"
        try:
            result = await self.db.query(query)
            count = 0
            # Handle result parsing for different response formats
            if isinstance(result, list) and result:
                first = result[0]
                if isinstance(first, dict):
                    # Direct format: [{'count': 10}]
                    if "count" in first:
                        count = first["count"]
                    # Nested format: [{"result": [{"count": 10}]}]
                    elif "result" in first:
                        count_data = first["result"]
                        if count_data and isinstance(count_data[0], dict):
                            count = count_data[0].get("count", 0)
            print(f"[+] Pre-run: {count} chains exist in DB")
            return {"pre_chain_count": count}
        except Exception as e:
            print(f"[!] Warning: Could not verify pre-state: {e}")
            return {"pre_chain_count": -1, "error": str(e)}

    async def verify_post_state(self, pre_count: int) -> dict:
        """Verify DB state after run."""
        print("[*] Verifying post-run DB state...")
        query = "SELECT count() FROM causal_chain GROUP ALL;"
        try:
            result = await self.db.query(query)
            post_count = 0
            # Handle result parsing for different response formats
            if isinstance(result, list) and result:
                first = result[0]
                if isinstance(first, dict):
                    # Direct format: [{'count': 10}]
                    if "count" in first:
                        post_count = first["count"]
                    # Nested format: [{"result": [{"count": 10}]}]
                    elif "result" in first:
                        count_data = first["result"]
                        if count_data and isinstance(count_data[0], dict):
                            post_count = count_data[0].get("count", 0)
            
            delta = post_count - pre_count
            print(f"[+] Post-run: {post_count} chains exist (delta: {delta:+d})")
            return {"post_chain_count": post_count, "delta": delta}
        except Exception as e:
            print(f"[!] Warning: Could not verify post-state: {e}")
            return {"post_chain_count": -1, "error": str(e)}

    async def load_causal_links(self) -> list[dict]:
        """Load all causal links from SurrealDB."""
        print("[*] Loading causal links from SurrealDB...")
        query = "SELECT * FROM causal_link ORDER BY confidence DESC;"
        try:
            result = await self.db.query(query)
            links = []
            if isinstance(result, list):
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    links = result[0]["result"]
                else:
                    links = result

            self.stats["links_loaded"] = len(links)
            print(f"[+] Loaded {len(links)} causal links")
            return links
        except Exception as e:
            print(f"[!] Error loading causal links: {e}")
            raise

    def build_graph(self, links: list[dict]):
        """Build adjacency lists from causal links."""
        print("[*] Building causal graph...")
        self.graph.clear()
        self.reverse_graph.clear()
        self.link_data.clear()

        for link in links:
            link_id = str(link.get("id", ""))
            from_id = str(link.get("from", ""))
            to_id = str(link.get("to", ""))
            confidence = float(link.get("confidence", 0.0))
            mechanism = link.get("mechanism", "")

            if not from_id or not to_id:
                continue

            # Store link metadata
            self.link_data[link_id] = {
                "from": from_id,
                "to": to_id,
                "confidence": confidence,
                "mechanism": mechanism,
            }

            # Build adjacency lists
            self.graph[from_id].append((to_id, link_id, confidence))
            self.reverse_graph[to_id].append((from_id, link_id, confidence))

        num_nodes = len(set(self.graph.keys()) | set(self.reverse_graph.keys()))
        print(f"[+] Graph built: {num_nodes} nodes, {len(links)} edges")

    def find_all_paths(
        self,
        start: str,
        end: str,
        max_length: int,
        path: list,
        visited: set,
    ) -> list[list]:
        """Find all paths from start to end within max_length using DFS."""
        paths = []

        if len(path) >= max_length:
            return paths

        if start == end and len(path) >= 2:
            paths.append(path[:])
            return paths

        for next_node, link_id, confidence in self.graph[start]:
            if next_node in visited and len(path) > 2:
                continue

            path.append((next_node, link_id, confidence))
            visited.add(next_node)

            paths.extend(self.find_all_paths(next_node, end, max_length, path, visited))

            path.pop()
            visited.discard(next_node)

        return paths

    def discover_chains(self, min_probability: float = 0.0) -> list[dict]:
        """Discover all chains of length 2+ in the graph."""
        print(f"[*] Discovering chains (max_length={MAX_CHAIN_LENGTH}, min_prob={min_probability:.2f})...")
        chains = []

        # Find all unique start and end nodes
        start_nodes = set(self.graph.keys())
        end_nodes = set(self.reverse_graph.keys())

        # For each potential chain from start to end
        for start in start_nodes:
            for end in end_nodes:
                if start == end:
                    continue

                # Find all paths
                paths = self.find_all_paths(
                    start, end, MAX_CHAIN_LENGTH + 1, [], {start}
                )

                for path in paths:
                    if len(path) < 2:  # Need at least 2 links (3 events)
                        continue

                    # Calculate chain probability as product of link confidences
                    probability = 1.0
                    for _, _, conf in path:
                        probability *= conf

                    if probability < min_probability:
                        continue

                    # Extract chain info
                    chain_links = [link_id for _, link_id, _ in path]
                    first_event = path[0][0]
                    last_event = path[-1][0]
                    chain_length = len(path)

                    # Build mechanism explanation
                    mechanism = self._build_chain_mechanism(path)

                    chains.append({
                        "from_event": first_event,
                        "to_event": last_event,
                        "links": chain_links,
                        "chain_length": chain_length,
                        "probability": probability,
                        "mechanism": mechanism,
                    })

        self.stats["chains_discovered"] = len(chains)
        print(f"[+] Discovered {len(chains)} candidate chains")
        return chains

    def _build_chain_mechanism(self, path: list) -> str:
        """Build a human-readable mechanism explanation for a chain."""
        if len(path) < 2:
            return ""

        mechanisms = []
        for i, (event_id, link_id, conf) in enumerate(path):
            link_info = self.link_data.get(link_id, {})
            mech = link_info.get("mechanism", "unknown mechanism")
            # Truncate and clean
            mech_clean = mech[:200].replace("\n", " ").strip()
            mechanisms.append(f"[Link {i+1} (conf={conf:.2f}): {mech_clean}]")

        return " → ".join(mechanisms)

    def get_chain_key(self, chain: dict) -> str:
        """Generate deterministic deduplication key for a chain."""
        from_id = chain.get("from_event", "")
        to_id = chain.get("to_event", "")
        links_tuple = tuple(chain.get("links", []))
        return f"{from_id}->{to_id}@{links_tuple}"

    async def check_chain_exists(self, from_id: str, to_id: str, links: list) -> bool:
        """Check if chain already exists in database (match by start_event, end_event, and steps)."""
        # Build query to check for existing chain with same steps
        steps_len = len(links)
        query = f"""
            SELECT id FROM causal_chain 
            WHERE start_event = {from_id} AND end_event = {to_id} 
            AND array::len(steps) = {steps_len}
            LIMIT 1;
        """
        try:
            result = await self.db.query(query)
            if isinstance(result, list) and result:
                if isinstance(result[0], dict) and "result" in result[0]:
                    return len(result[0]["result"]) > 0
                return len(result) > 0
            return False
        except Exception:
            return False

    async def write_causal_chain(
        self,
        from_id: str,
        to_id: str,
        links: list,
        chain_length: int,
        probability: float,
        mechanism: str,
    ) -> bool:
        """Write a causal chain to SurrealDB."""
        # Format steps as SurrealDB native array syntax (not JSON)
        # SurrealDB expects: [record:table:id, record:table:id]
        steps_str = "[" + ", ".join(links) + "]"
        clean_mechanism = mechanism.replace('"', '\\"').replace('\n', ' ')[:2000]

        query = f"""
            CREATE causal_chain SET
            start_event = {from_id},
            end_event = {to_id},
            steps = {steps_str},
            coherence = {probability},
            total_confidence = {probability},
            mechanism = "{clean_mechanism}";
        """

        try:
            result = await self.db.query(query)
            # Check if the query returned an error
            if isinstance(result, str) and "error" in result.lower():
                print(f"      [!] DB write error: {result}")
                self.stats["errors"].append({
                    "type": "db_write",
                    "from": from_id,
                    "to": to_id,
                    "error": str(result),
                })
                return False
            return True
        except Exception as e:
            print(f"      [!] DB write error: {e}")
            self.stats["errors"].append({
                "type": "db_write",
                "from": from_id,
                "to": to_id,
                "error": str(e),
            })
            return False

    async def process_chains(self, chains: list[dict]) -> list[dict]:
        """Process discovered chains and filter/accept them."""
        print(f"[*] Processing {len(chains)} candidate chains...")

        accepted = []
        for i, chain in enumerate(chains):
            chain_key = self.get_chain_key(chain)

            # Check for duplicates within run
            if chain_key in self.seen_chain_keys:
                self.stats["chains_duplicated"] += 1
                print(f"  [-] Duplicate chain (in-run): {chain['from_event']} -> {chain['to_event']}")
                continue
            self.seen_chain_keys.add(chain_key)

            from_id = chain["from_event"]
            to_id = chain["to_event"]
            links = chain["links"]
            chain_length = chain["chain_length"]
            probability = chain["probability"]
            mechanism = chain["mechanism"]

            # Check against DB
            if not DRY_RUN:
                exists = await self.check_chain_exists(from_id, to_id, links)
                if exists:
                    print(f"  [-] Chain already exists in DB: {from_id} -> {to_id}")
                    self.stats["chains_duplicated"] += 1
                    continue

                # Write to database
                success = await self.write_causal_chain(
                    from_id, to_id, links, chain_length, probability, mechanism
                )
                if success:
                    print(f"  [+] Chain accepted and written: {from_id} -> {to_id} (p={probability:.3f}, len={chain_length})")
                    self.stats["chains_accepted"] += 1
                    self.stats["db_writes"] += 1
                    accepted.append({
                        "from": from_id,
                        "to": to_id,
                        "links": links,
                        "chain_length": chain_length,
                        "probability": probability,
                        "mechanism": mechanism,
                    })
            else:
                print(f"  [+] Chain accepted (Dry Run): {from_id} -> {to_id} (p={probability:.3f}, len={chain_length})")
                self.stats["chains_accepted"] += 1
                accepted.append({
                    "from": from_id,
                    "to": to_id,
                    "links": links,
                    "chain_length": chain_length,
                    "probability": probability,
                    "mechanism": mechanism,
                })

        return accepted

    async def run(self) -> dict:
        """Execute the full chain discovery pipeline."""
        print("=" * 60)
        print("Volva Chain Discoverer - Phase 3")
        print("=" * 60)
        print(f"Mode: {'DRY_RUN' if DRY_RUN else 'PRODUCTION'}")
        print(f"Max chain length: {MAX_CHAIN_LENGTH}")
        print(f"Min chain probability: {MIN_CHAIN_PROBABILITY}")

        await self.connect()

        try:
            # Verify pre-state
            pre_state = await self.verify_pre_state()

            # Load and build graph
            links = await self.load_causal_links()
            if not links:
                print("[!] No causal links found in database")
                return {"stats": self.stats, "accepted_chains": [], "pre_state": pre_state}

            self.build_graph(links)

            # Discover chains
            chains = self.discover_chains(min_probability=MIN_CHAIN_PROBABILITY)
            if not chains:
                print("[!] No chains discovered")
                post_state = await self.verify_post_state(pre_state["pre_chain_count"])
                return {"stats": self.stats, "accepted_chains": [], "pre_state": pre_state, "post_state": post_state}

            # Process chains
            start_time = time.time()
            accepted = await self.process_chains(chains)
            elapsed = time.time() - start_time

            # Verify post-state
            post_state = await self.verify_post_state(pre_state["pre_chain_count"])

            # Summary
            print()
            print("-" * 60)
            print("RUN SUMMARY")
            print("-" * 60)
            print(f"Links loaded: {self.stats['links_loaded']}")
            print(f"Chains discovered: {self.stats['chains_discovered']}")
            print(f"Chains accepted: {self.stats['chains_accepted']}")
            print(f"Chains rejected: {self.stats['chains_rejected']}")
            print(f"Chains duplicated: {self.stats['chains_duplicated']}")
            print(f"DB writes: {self.stats['db_writes']}")
            print(f"Elapsed time: {elapsed:.1f}s")
            print("-" * 60)

            # Reproduction command
            repro_cmd = f"DRY_RUN={'true' if DRY_RUN else 'false'} python3 chain_discoverer.py"
            print(f"\nReproduction: {repro_cmd}")

            return {
                "run_id": datetime.now(timezone.utc).isoformat(),
                "mode": "dry_run" if DRY_RUN else "production",
                "stats": self.stats,
                "accepted_chains": accepted,
                "pre_state": pre_state,
                "post_state": post_state,
                "elapsed_seconds": elapsed,
                "reproduction_command": repro_cmd,
            }
        finally:
            await self.close()


async def main():
    """Entry point."""
    discoverer = ChainDiscoverer()

    try:
        result = await discoverer.run()

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
