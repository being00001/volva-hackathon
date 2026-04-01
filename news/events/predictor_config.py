"""
Prediction Engine Configuration for Volva Phase 2
Uses SurrealDB for event/causal link storage and Gemini 2.5 Flash for prediction generation.
"""

# SurrealDB Connection Settings (reused from config.py)
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# Gemini API Settings
GEMINI_API_KEY_ENV = "GOOGLE_AIS_API_KEY"

# Phase 6: External Data API Keys (for ingest_indicators.py)
# Set these environment variables before running ingest_indicators.py:
#   export FRED_API_KEY="your_fred_api_key"      # https://fred.stlouisfed.org/docs/api/api_key.html
#   export FMP_API_KEY="your_fmp_api_key"        # https://site.financialmodelingprep.com/
FRED_API_KEY_ENV = "FRED_API_KEY"
FMP_API_KEY_ENV = "FMP_API_KEY"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MAX_TOKENS = 4096  # Increased from 2048 to handle long JSON responses

# Prediction Engine Settings
PREDICTION_WINDOW_HOURS = 72  # Look back window for triggering events (hours)
PREDICTION_HORIZON_DAYS = 14  # How far ahead predictions can extend (days)
MIN_CONFIDENCE_THRESHOLD = 0.75  # Minimum causal link confidence to consider
MAX_PREDICTIONS_PER_RUN = 10  # Limit predictions per execution
MAX_CANDIDATE_EVENTS = 50  # Max recent events to consider
MAX_CANDIDATE_LINKS = 20   # Max causal links to consider
MAX_RETRIES = 3  # Max retries for failed API calls
RETRY_BACKOFF = 1  # Base backoff in seconds

# Run settings
DRY_RUN = False  # Production mode - write to DB
VERBOSE = True  # Print detailed progress
AGENT_NAME = "volva-predictor-v1"  # Identifier for this prediction agent

# Official Contract Addresses (Base Sepolia)
LETHE_ESCROW_ADDRESS = "0x1036522C3703971cc3c3c0ead908eAe52c1ba328"
BROKER_REGISTRY_ADDRESS = "0x2A0e5D413Ed7C5dC760f4680a0471d474397253b"
BASE_SEPOLIA_RPC = "https://sepolia.base.org"

# Phase 6: Category-to-Indicator Mapping
# Maps news categories to relevant indicator series for prediction context.
# Categories are matched against event content via keyword detection.
CATEGORY_INDICATOR_MAP = {
    "inflation": [
        "FRED:FEDFUNDS", "FRED:CPIAUCSL", "FRED:CPILFESL",
        "FRED:PCE", "FRED:PCETRIM12M1S"
    ],
    "central_bank": [
        "FRED:FEDFUNDS", "FRED:DFF", "FRED:T10Y3M", "FRED:T10Y2Y"
    ],
    "employment": [
        "FRED:UNRATE", "FRED:PAYEMS", "FRED:JTSJOL"
    ],
    "housing": [
        "FRED:HOUST", "FRED:CSUSHPINSA"
    ],
    "gdp": [
        "FRED:GDP", "FRED:WALCL"
    ],
    "markets": [
        "FMP:^GSPC", "FMP:^IXIC", "FMP:^DJI"
    ],
    "energy": [
        "FMP:CL=F", "FMP:BZ=F", "FMP:NG=F"
    ],
    "metals": [
        "FMP:GC=F", "FMP:SI=F"
    ],
    "tech": [
        "FMP:AAPL", "FMP:MSFT", "FMP:NVDA", "FMP:TSLA", "FMP:AMZN"
    ],
}

# Category detection keywords (lowercase, matched against event content)
CATEGORY_KEYWORDS = {
    "inflation": [
        "inflation", "cpi", "pce", "price index", "headline inflation",
        "core inflation", "cost of living", "price pressure", "hot economy"
    ],
    "central_bank": [
        "federal reserve", "fed", "monetary policy", "interest rate",
        "fed funds rate", "yield curve", "quantitative easing", "qe",
        "tapering", "balance sheet", "powell"
    ],
    "employment": [
        "unemployment", "jobs", "payroll", "labor market", "hiring",
        "firing", "workforce", "employment", "jobs report", "nonfarm"
    ],
    "housing": [
        "housing", "real estate", "home price", "mortgage", "starts",
        "case-shiller", "construction", "residential"
    ],
    "gdp": [
        "gdp", "gross domestic product", "economic growth", "recession",
        "economic output", "productivity"
    ],
    "markets": [
        "s&p", "sp500", "nasdaq", "dow jones", "stock market", "equities",
        "market sentiment", "bull market", "bear market"
    ],
    "energy": [
        "oil", "crude", "petroleum", "natural gas", "energy prices",
        "opec", "wti", "brent", "gasoline"
    ],
    "metals": [
        "gold", "silver", "precious metals", "copper", "commodities"
    ],
    "tech": [
        "apple", "microsoft", "nvidia", "tesla", "amazon", "faang",
        "tech stocks", "big tech", "silicon valley"
    ],
}
