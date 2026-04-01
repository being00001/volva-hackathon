"""
Chain Discoverer Configuration for Volva Phase 3
Discovers transitive causal chains from direct causal links.
"""

# SurrealDB Connection Settings
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# Chain Discovery Settings
MAX_CHAIN_LENGTH = 5  # Maximum chain length (number of links)
MIN_CHAIN_PROBABILITY = 0.5  # Minimum probability threshold for chain acceptance

# Run settings
DRY_RUN = False  # Production mode - write to DB
VERBOSE = True  # Print detailed progress
