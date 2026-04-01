"""
Causal Link Discovery Configuration for Volva
Uses SurrealDB for event storage and Gemini 2.0 Flash for causal inference.
"""

# SurrealDB Connection Settings (reused from config.py)
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# Gemini API Settings
GEMINI_API_KEY_ENV = "GOOGLE_AIS_API_KEY"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

# Causal Link Discovery Settings
WINDOW_HOURS = 24  # Sliding window in hours
CONFIDENCE_THRESHOLD = 0.70  # Minimum confidence to accept a link
MAX_CANDIDATE_PAIRS = 100  # Limit for production runs
BATCH_SIZE = 5  # Number of pairs to score in one Gemini request
MAX_RETRIES = 3  # Max retries for failed API calls
RETRY_BACKOFF = 1  # Base backoff in seconds

# Run settings
DRY_RUN = False  # Production mode - write to DB
VERBOSE = True  # Print detailed progress

# Phase 6: Category-to-Indicator Mapping
# Maps news categories to relevant indicator series for causal reasoning context.
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
