# Volva — Causal AI Trading Agent
## Lablab.ai AI Trading Agents Hackathon Submission

---

## Slide 1: Title
# Volva
### Causal AI Trading Agent
*Seeing the threads that bind cause to effect*

- Team: Volva
- Hackathon: Lablab.ai AI Trading Agents
- Repo: github.com/being00001/volva-trading-agent

---

## Slide 2: The Problem
# Current Trading Bots Are Blind

- **Technical analysis only** — patterns without understanding
- **Correlation ≠ Causation** — spurious signals lead to losses  
- **Black box predictions** — no explainability, no trust
- **Reactive, not predictive** — respond to price, not to causes

> What if an AI could understand *why* markets move?

---

## Slide 3: Our Solution
# Causal Graph Engine

Volva builds a **directed causal graph** connecting real-world events to market outcomes:

```
Fed Rate Decision ──→ USD Strength ──→ BTC Price Drop
       ↓                    ↓
Bond Yields Rise    Risk-Off Sentiment
       ↓                    ↓
   └──────→ ETH/SOL Sell Pressure ──────┘
```

**Key insight**: Understanding causal chains enables *predictive* (not reactive) trading.

---

## Slide 4: Architecture
# System Design

| Layer | Component | Technology |
|-------|-----------|-----------|
| **Data** | Multi-source ingestion | RSS, FMP, FRED, Grok |
| **Knowledge** | Causal graph database | SurrealDB |
| **Intelligence** | Causal analysis + prediction | Gemini 2.0/2.5 |
| **Execution** | Paper trading | Kraken CLI |

Pipeline: Events → Causal Links → Predictions → Trade Signals → Execution

---

## Slide 5: Results
# Performance Metrics

| Metric | Value |
|--------|-------|
| Events ingested | 1,869 |
| Causal links discovered | 39 |
| Graph density | 2.09% |
| Prediction hit rate | 77%+ |
| Trading pairs | BTC/USD, ETH/USD, SOL/USD |
| Oracle signing | EIP-191 (Base-ready) |

**Virtual ledger** tracks all predictions with hit/miss feedback for continuous improvement.

---

## Slide 6: Demo & Future
# What's Next

**Demo**: Live causal prediction → paper trade execution

**Roadmap**:
1. 🔜 Live trading with real capital
2. 🔜 Multi-exchange support (CEX + DEX)
3. 🔜 On-chain oracle settlement (Base/Lethe)
4. 🔜 Multi-asset expansion (forex, commodities)

**Open source**: github.com/being00001/volva-trading-agent (MIT)

---

*Volva — Where causality meets alpha*
