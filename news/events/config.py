"""
RSS Feed Sources Configuration for Volva Data Pipeline
Prioritized for causal signal detection (world events, tech, economics)
"""

# SurrealDB Connection Settings
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

# RSS Feed Sources
# Each source has: name, url, categories (for causal signal tagging)
RSS_FEEDS = [
    {
        "name": "BBC World News",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "type": "rss",
        "reliability": 0.7,
        "categories": ["world", "politics", "economy"],
    },
    {
        "name": "Al Jazeera World",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "type": "rss",
        "reliability": 0.7,
        "categories": ["world", "politics", "economy", "middleeast"],
    },
    {
        "name": "TechCrunch",
        "url": "https://techcrunch.com/feed/",
        "type": "rss",
        "reliability": 0.6,
        "categories": ["tech", "startups", "ai"],
    },
    {
        "name": "Hacker News Frontpage",
        "url": "https://hnrss.org/frontpage",
        "type": "rss",
        "reliability": 0.5,
        "categories": ["tech", "programming", "startups"],
    },
]

# Pagination/Limit settings
MAX_ITEMS_PER_FEED = 50  # Max items to process per feed per run
