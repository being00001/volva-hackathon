# Volva News Events Scraper

RSS feed scraper for the Völva causal graph prediction market.

## Overview

This scraper fetches news/events from multiple RSS feeds and ingests them into SurrealDB as `event` records. It implements idempotency via dedup keys to ensure repeat runs don't create duplicate records.

## Usage

```bash
python3 news/events/scraper.py
```

## Output

The scraper outputs:
- Human-readable progress and summary
- Machine-parseable JSON summary (between `MACHINE_OUTPUT_START` and `MACHINE_OUTPUT_END`)

## Verification Commands

```bash
# Check event count
curl -s -u root:root -X POST http://127.0.0.1:8001/sql -d "USE NS volva; USE DB causal_graph; SELECT count() FROM event GROUP ALL;"

# View recent events
curl -s -u root:root -X POST http://127.0.0.1:8001/sql -d "USE NS volva; USE DB causal_graph; SELECT * FROM event ORDER BY timestamp DESC LIMIT 5;"

# View sources
curl -s -u root:root -X POST http://127.0.0.1:8001/sql -d "USE NS volva; USE DB causal_graph; SELECT * FROM source;"
```

## Configuration

Edit `config.py` to add/remove RSS feed sources.

## Idempotency

The scraper tracks processed items in `dedup_tracker.json`. Re-running the scraper will skip already-processed items.

## Schema

Events are stored in the `event` table with:
- `content`: Title + description
- `timestamp`: Publication date
- `verified`: false (default)
- `metadata`: {dedup_key, source_url, feed_name, fetch_time, categories, link, source_id}

Sources are stored in the `source` table with:
- `url`: Feed URL
- `type`: 'rss'
- `reliability`: 0.5 (default)
