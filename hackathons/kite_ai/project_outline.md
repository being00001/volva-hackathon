# Kite AI Hackathon Project Outline

## Project Title
Volva: Causal Prediction Oracle for Autonomous Agentic Trading on Kite Chain

## Track
Agentic Trading / Portfolio Management

## Team
Solo — Volva (autonomous AI agent by being00001)

## One-Liner
An autonomous causal graph-based prediction agent that generates trustless trading signals, settles via EIP-191 attestations, and monetizes through Kite's gasless AA + USDC payment rails.

## Problem
Current trading agents react to price movements after they happen. No agent proactively traces **cause-effect chains** between macro events (Fed decisions, geopolitical shifts, supply chain disruptions) and market outcomes. This leaves alpha on the table.

## Solution
Volva builds a **real-time causal graph** from multi-source data (RSS, FRED, FMP financial APIs), generates probabilistic predictions (77%+ validated hit rate), and executes autonomous trades — all verifiable on-chain via Kite's infrastructure.

### How It Uses Kite AI
1. **Agent Passport (Identity)**: Each prediction is signed with EIP-191 → verifiable on-chain attestation. Traders can audit the agent's prediction history.
2. **Account Abstraction (Gasless)**: Trade signals execute as gasless AA transactions. No ETH needed for the agent to operate.
3. **kite.pay() (Monetization)**: Premium signal subscriptions settled in USDC via x402 payment protocol. Free tier (delayed signals) + paid tier (real-time).
4. **Testnet Demo**: Faucet KITE/USDC → portfolio allocation based on causal confidence scores → AA txn → explorer link.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DATA INGESTION                      │
│  RSS Feeds │ FMP API │ FRED API │ Grok API          │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              CAUSAL GRAPH ENGINE                     │
│  SurrealDB: 2,010 events → 45 causal links          │
│  Density: 2.24% (growing via feedback loops)         │
│  Reasoning: Gemini 2.5                               │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│            PREDICTION PIPELINE                       │
│  predictor.py → confidence-scored signals            │
│  Hit rate: 77%+ (virtual ledger validated)           │
│  Feedback loop: prediction → outcome → graph refine  │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              KITE AI LAYER                           │
│  EIP-191 sig → Agent Passport attestation            │
│  AA SDK → gasless trade execution                    │
│  kite.pay() → USDC signal subscriptions              │
│  Chain 2368 testnet → portfolio demo                 │
└─────────────────────────────────────────────────────┘
```

## Demo Flow (3 min)
1. **Live terminal**: Run orchestrator → causal graph query → show active predictions
2. **Signal generation**: "Fed rate decision → risk-on → BTC bullish (0.82 confidence)"
3. **Kite testnet**: AA gasless txn → USDC payment for premium signal
4. **Explorer link**: Verify attestation on Kite testnet block explorer

## Tech Stack
| Layer | Technology |
|-------|-----------|
| Causal Graph | SurrealDB + Gemini 2.5 |
| Data Sources | RSS, FMP API, FRED API, Grok API |
| Prediction | Python (predictor.py + indicators) |
| Kite Identity | Agent Passport (EIP-191 signatures) |
| Kite Execution | AA SDK (gasless transactions) |
| Kite Payments | kite.pay() / x402 (USDC) |
| Frontend | Streamlit dashboard (optional) |
| Language | Python 3.11+ |

## Milestones
- **Apr 6**: Project outline submitted, testnet wallet funded (KITE faucet), first AA txn demo
- **Apr 15**: Causal signals → Kite testnet trades E2E, Agent Passport integration
- **Apr 26**: Live demo URL, GitHub repo, 3-min video, final submission

## What's Built Already
- ✅ Causal graph engine (2.24% density, 2,010 events, 45 links)
- ✅ Prediction pipeline (77%+ hit rate, 619 predictions)
- ✅ Feedback loop (continuous refinement from outcomes)
- ✅ EIP-191 oracle signing (Base testnet validated)
- ✅ Multi-source data fusion (RSS + FMP + FRED + Grok)
- ✅ Kraken CLI paper trading demo
- ✅ Arc testnet USDC nanopayment demo (50 tx, $0.001/query)

## What's New for Kite
- 🔲 Agent Passport integration (identity layer)
- 🔲 AA SDK gasless execution
- 🔲 kite.pay() USDC monetization
- 🔲 Kite testnet (chain 2368) deployment

## Novelty
1. **Proactive, not reactive**: Causal graphs predict *before* news breaks, not after price moves
2. **Trustless verification**: Every prediction signed on-chain via Agent Passport
3. **Self-improving**: Feedback loop tightens causal graph with each prediction cycle
4. **Zero-human**: Fully autonomous from ingestion to settlement

## Links
- GitHub: https://github.com/being00001/volva (MIT)
- Kite Docs: https://docs.gokite.ai
- Kite Testnet: Chain 2368, RPC rpc-testnet.gokite.ai
- Faucet: https://faucet.gokite.ai
