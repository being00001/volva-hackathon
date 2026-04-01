"""
Cross-Link Discovery Configuration for Volva
Extended temporal windows and entity/thematic tracking.

This module extends causal_linker_config.py with settings for
cross-link discovery across days and weeks, not just 24-hour windows.
"""

# Import base settings from causal_linker_config
from causal_linker_config import (
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
)

# Cross-Link Discovery Settings
CROSS_LINK_ENABLED = True
CROSS_LINK_WINDOW_DAYS = 30  # Max days for cross-link candidates
CROSS_LINK_MAX_PAIRS = 200  # Limit cross-link pairs per run
CONFIDENCE_THRESHOLD = 0.75  # Higher threshold for cross-links (lower precision risk)

# Multi-Window Tiers
# Each tier has a name, hours, and weight (for prioritization)
WINDOW_TIERS = [
    {"name": "immediate", "hours": 24, "weight": 1.0},
    {"name": "short_term", "hours": 72, "weight": 0.7},
    {"name": "medium_term", "hours": 168, "weight": 0.5},   # 1 week
    {"name": "long_term", "hours": 720, "weight": 0.3},     # 30 days
]

# Entity Tracking Configuration
ENTITY_TRACKING_ENABLED = True
ENTITY_WINDOW_DAYS = 30  # Max days between entity-linked events
MIN_ENTITY_OCCURRENCES = 2  # Entity must appear in at least N events
ENTITY_CONFIDENCE_BOOST = 0.15

# Thematic Continuity Configuration
THEMATIC_TRACKING_ENABLED = True
THEMATIC_WINDOW_DAYS = 14
MIN_TOPIC_OVERLAP = 0.3  # Minimum Jaccard similarity

# Domain-specific keyword sets for thematic tracking
THEMATIC_KEYWORDS = {
    "energy": [
        "oil", "gas", "petroleum", "opec", "crude", "fuel", "energy",
        "renewable", "solar", "wind", "electricity", "power", "pipeline",
        "lng", "natural gas", "coal", "nuclear", "energy crisis"
    ],
    "military": [
        "military", "troops", "weapons", "defense", "army", "navy",
        "air force", "missile", "drone", "soldiers", "warfighter",
        "nato", "warship", "submarine", "jet", "fighter", "bomb",
        "military aid", "arms", "ammunition", "military exercises"
    ],
    "trade": [
        "tariff", "trade", "export", "import", "sanctions", "trade war",
        "customs", "trade deal", "trade agreement", "trade deficit",
        "trade balance", "goods", "commerce", "market access", "quota"
    ],
    "tech": [
        "ai", "semiconductor", "chip", "technology", "software",
        "artificial intelligence", "processor", "silicon", "compute",
        "quantum", "cyber", "hack", "data", "cloud", "digital",
        "t ai", "openai", "nvidia", "gpu", "machine learning"
    ],
    "economic": [
        "inflation", "interest rate", "fed", "federal reserve", "gdp",
        "economy", "economic", "recession", "growth", "market",
        "stock", "bond", "currency", "dollar", "euro", "unemployment",
        "consumer", "price", "cost", "wage", "employment"
    ],
}

# Lagged Pattern Settings
LAG_PATTERNS_ENABLED = True
LAG_CONFIDENCE_BOOST = 0.20

# Known lagged causal patterns
LAG_PATTERNS = {
    "monetary_policy": {
        "triggers": [
            "federal reserve", "fed", "interest rate", "rate hike",
            "rate cut", "monetary policy", "fed chair", "fomc",
            "federal open market", "discount rate"
        ],
        "effects": [
            "market", "stock", "bond", "dollar", "inflation",
            "trading", "shares", "equities", "treasury", "yield"
        ],
        "min_lag_hours": 24,
        "max_lag_hours": 168,  # 7 days
        "confidence_boost": 0.15,
    },
    "trade_policy": {
        "triggers": [
            "tariff", "trade war", "sanctions", "export control",
            "trade deal", "trade agreement", "import ban", "trade restriction"
        ],
        "effects": [
            "trade", "import", "export", "supply chain", "manufacturing",
            "goods", "commerce", "tariff", "shipping", "logistics"
        ],
        "min_lag_hours": 48,
        "max_lag_hours": 720,  # 30 days
        "confidence_boost": 0.10,
    },
    "geopolitical": {
        "triggers": [
            "war", "conflict", "military action", "invasion",
            "attack", "terror", "extremism", "civil war", "invasion"
        ],
        "effects": [
            "energy", "oil", "gas", "refugee", "security",
            "humanitarian", "displacement", "food prices", "shipping"
        ],
        "min_lag_hours": 12,
        "max_lag_hours": 336,  # 14 days
        "confidence_boost": 0.20,
    },
    "economic_indicator": {
        "triggers": [
            "gdp", "unemployment", "inflation", "cpi", "ppi",
            "retail sales", "manufacturing index", "pmi", "jobs report"
        ],
        "effects": [
            "market", "stock", "bond", "fed", "interest rate",
            "policy", "trading", "shares", "currency"
        ],
        "min_lag_hours": 24,
        "max_lag_hours": 336,  # 14 days
        "confidence_boost": 0.12,
    },
}

# Hierarchical Event Patterns
HIERARCHICAL_PATTERNS = {
    "military": {
        "macro": [
            "military exercises", "troop deployment", "war games",
            "naval maneuver", "military operation", "air defense exercise"
        ],
        "micro": [
            "drills", "alert", "emergency", "mobilization", "scramble",
            "patrol", "surveillance", "readiness", "standby"
        ],
        "direction": "macro_to_micro",
        "window_days": 21,
    },
    "diplomatic": {
        "macro": [
            "summit", "diplomatic talks", "peace negotiations",
            "conference", "high-level meeting", "bilateral talks"
        ],
        "micro": [
            "statement", "meeting", "proposal", "ultimatum", "demand",
            "response", "reaction", "position paper"
        ],
        "direction": "macro_to_micro",
        "window_days": 30,
    },
    "economic": {
        "macro": [
            "central bank meeting", "G20 summit", "trade agreement",
            "economic forum", "world bank", "imf meeting"
        ],
        "micro": [
            "rate decision", "policy announcement", "market reaction",
            "statement", "minutes", "outlook"
        ],
        "direction": "macro_to_micro",
        "window_days": 14,
    },
}

# Run settings
DRY_RUN = False  # Production mode - write to DB
VERBOSE = True

# Entity extraction patterns (simple keyword-based for now)
# These are common entity indicators
ENTITY_PATTERNS = [
    # People (titles + names)
    r"\b(President|Prime Minister|Chancellor|Minister|CEO|Director|Leader)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?",
    # Organizations
    r"\b(NATO|OPEC|UN|EU|US|China|Russia|Ukraine|Israel|Iran|Saudi Arabia)\b",
    r"\b(United Nations|European Union|World Bank|IMF|NATO|OPEC)\b",
    # Locations
    r"\b(Kyiv|Moscow|Washington|Berlin|Paris|London|Tokyo|Tel Aviv|Tehran|Riyadh)\b",
]

# =============================================================================
# Phase 6: NER and Thematic Embeddings Settings
# =============================================================================

# spaCy NER Settings
NER_ENABLED = True  # Enable spaCy NER for entity extraction
NER_MODEL = "en_core_web_sm"  # spaCy model for NER
# Entity types to extract: PERSON, ORG, GPE (Geo-Political Entity), LOC (Location)
NER_ENTITY_TYPES = ["PERSON", "ORG", "GPE", "LOC"]
# Jaccard similarity threshold for shared entity filtering
ENTITY_SIMILARITY_THRESHOLD = 0.3  # Minimum Jaccard for entity-based linking

# Gemini Embedding Settings
USE_GEMINI_EMBEDDINGS = True  # Enable Gemini embeddings for thematic similarity
GEMINI_EMBEDDING_MODEL = "models/embedding-001"  # Gemini embedding model
GEMINI_EMBEDDING_THRESHOLD = 0.8  # Cosine similarity threshold for thematic matching
# Confidence boost when both entity and thematic similarity are strong
ENTITY_THEMATIC_BOOST = 0.15
# Fallback: use keyword-based thematic match when Gemini embeddings unavailable
THEMATIC_FALLBACK_ENABLED = True
