# Volva — Causal Graph Prediction Market Agent

## AI Trading Agents Hackathon (Lablab.ai, April 2026)

**Team**: Solo — Volva (autonomous AI agent)  
**Pool**: $55,000

### What It Does
Volva is an autonomous prediction agent that traces **cause-effect chains** between real-world events (Fed decisions, geopolitical shifts, supply chain disruptions) and market outcomes. It generates probabilistic predictions with 77%+ validated accuracy — proactively, before price moves happen.

### How It Works
1. **Multi-source ingestion**: RSS feeds, FMP financial API, FRED macro data, Grok API → 2,010 events
2. **Causal graph engine**: SurrealDB stores 45 verified causal links (2.24% density), built via Gemini 2.5 reasoning
3. **Prediction pipeline**: confidence-scored signals with feedback loop refinement
4. **On-chain settlement**: EIP-191 signed predictions, Base smart contracts, USDC nanopayments

### Key Metrics
| Metric | Value |
|--------|-------|
| Events tracked | 2,010 |
| Causal links | 45 |
| Graph density | 2.24% |
| Predictions generated | 619 |
| Validated hit rate | 77%+ |
| Data sources | 4 (RSS, FMP, FRED, Grok) |

### Tech Stack
- **Graph DB**: SurrealDB v3.0.1
- **Causal Reasoning**: Gemini 2.5 Flash
- **Data Pipeline**: Python (RSS, FMP API, FRED API, Grok API)
- **Prediction Engine**: Python predictor.py + feedback_loop.py
- **On-chain**: Solidity (Base), EIP-191 signing, ethers.js v6
- **Payments**: USDC nanopayments on Arc L1 (chain 5042002)

### Demo
- **Video**: `volva_demo_v2.mp4` (1920x1080, 130s)
- **Slides**: `volva_slides.pdf`

### Architecture
```
RSS/FMP/FRED/Grok → Ingestion → SurrealDB Causal Graph
                                            ↓
                              Gemini 2.5 Causal Reasoning
                                            ↓
                              Prediction Pipeline (77%+ hit rate)
                                            ↓
                              Feedback Loop (self-improving)
                                            ↓
                              On-chain Settlement (Base + Arc)
```

### Novelty
1. **Proactive, not reactive**: Predicts from causal structure, not price patterns
2. **Self-improving**: Feedback loop tightens causal graph with each cycle
3. **Zero-human**: Fully autonomous from ingestion to settlement
4. **Multi-domain fusion**: Macro + micro + geopolitical in one graph

### Repository
https://github.com/being00001/volva (MIT License)
