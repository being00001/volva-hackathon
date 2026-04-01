# Völva — Causal Graph Prediction Market

*"Trace the threads that bind cause to effect"*

Völva is a causal graph prediction market system that discovers causal relationships between events and uses them to make predictions on Polymarket. The system continuously monitors global events, builds a causal knowledge graph, and generates predictions that are posted to the Polymarket CLOB API.

## Key Stats

| Metric | Value |
|--------|-------|
| Events Processed | 2,010 |
| Causal Links Discovered | 45 |
| Graph Density | 2.24% |
| Prediction Hit Rate | 77.72% |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌──────────────┐
│ Data        │     │ Causal Graph │     │ Prediction │     │ Feedback     │
│ Pipeline    │────▶│ Construction │────▶│ Generator  │────▶│ Loop         │
│ (events)    │     │ (Gemini 2.5) │     │ (markets)  │     │ (on-chain)   │
└─────────────┘     └──────────────┘     └────────────┘     └──────────────┘
```

1. **Data Pipeline**: Ingests events from news feeds, financial data APIs, and economic indicators
2. **Causal Graph Construction**: Uses Gemini 2.5 to discover causal relationships between events
3. **Prediction Generation**: Creates Polymarket questions based on causal chain outcomes
4. **Feedback Loop**: Monitors on-chain resolution and uses results to improve future predictions

## Tech Stack

- **Language**: Python 3.11+
- **Database**: SurrealDB (causal graph storage)
- **AI**: Google Gemini 2.5 (causal reasoning)
- **Prediction Market**: Polymarket CLOB API
- **Blockchain**: Base (settlement and verification)

## Setup

### Prerequisites

- Python 3.11+
- SurrealDB instance

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/volva-hackathon.git
cd volva-hackathon

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys
```

### Required API Keys

Create a `.env` file based on `.env.example` and add:

- `FMP_API_KEY` - Financial Modeling Prep (market data)
- `FRED_API_KEY` - FRED (economic indicators)
- `GOOGLE_AIS_API_KEY` - Google AI / Gemini (causal reasoning)
- `GROK_API_KEY` - Grok (optional, advanced analysis)

### Running

```bash
# Start SurrealDB
make install_surreal

# Run the causal discovery pipeline
python -m news.events.chain_discoverer

# Generate predictions
python -m news.events.predictor

# Push to Polymarket
python -m news.events.lethe_batch_live
```

## Project Structure

```
volva-hackathon/
├── contracts/           # Smart contracts for settlement
├── graphviz/            # Causal graph visualizations
├── hackathons/          # Hackathon submissions and demos
├── mermaid/             # Architecture diagrams
├── news/
│   └── events/          # Core event processing
│       ├── causal_linker.py      # Causal link discovery
│       ├── chain_discoverer.py    # Causal chain finding
│       ├── cross_linker.py        # Cross-reference linking
│       ├── predictor.py           # Market prediction
│       ├── feedback_loop.py       # Learning from outcomes
│       └── lethe_*/               # Polymarket integration
├── scripts/             # Utility scripts
├── submission/          # Submission materials
├── svg/                # Scalable graphics
├── schema.surrealql    # Database schema
├── Makefile            # Development tasks
└── requirements.txt    # Python dependencies
```

## Hackathon Submissions

This project has been submitted to:

- **Lablab.ai AI Trading Agents** - Causal AI for prediction markets
- **Kite AI** - Agentic Economy track
- **Agentic Economy on Arc** - Autonomous agent market making

## License

MIT License - See [LICENSE](LICENSE) for details.
