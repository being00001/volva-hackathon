# Volva Demo Video Script (~4:30)
## Lablab.ai AI Trading Agents Hackathon

---

## [0:00 - 0:30] Intro

**[터미널 화면, 깔끔한 배경]**

"Hi, I'm presenting Volva — a causal AI trading agent.

Most trading bots rely on technical indicators — moving averages, RSI, patterns. They see *what* happened but not *why*.

Volva is different. It builds a **causal graph** — a network of cause-effect relationships between real-world events — to understand *why* markets move, and predict what happens next.

Let me show you how it works."

---

## [0:30 - 1:30] Market Brief Demo

**[터미널에서 입력]**

```bash
cd volva-trading-agent
python3 kraken_demo.py
```

**[출력이 나오는 동안 설명]**

"First, Volva fetches real-time market data via Kraken CLI. 

Here we can see live prices for BTC, ETH, and SOL with 24-hour changes and volume. This is the data layer — but the real magic is what happens underneath."

---

## [1:30 - 3:00] Causal Prediction Engine

**[터미널에서 입력]**

```bash
python3 volka_kraken_agent.py --dry-run
```

**[시그널 출력이 나오는 동안 설명]**

"Now let's run the prediction engine in dry-run mode.

Volva has ingested **1,869 events** from RSS feeds, financial APIs, and on-chain data. These are connected by **39 causal links** — verified cause-effect relationships stored in a graph database.

The engine analyzes these causal chains to generate trading signals. 

[시그널 가리키며]
Here we see signals for BTC, ETH, and SOL — each with a direction, confidence score, and the causal events backing the prediction.

The hit rate across resolved predictions is **over 77 percent**. And importantly, every prediction is **explainable** — you can trace it back to specific causal events."

---

## [3:00 - 4:00] Architecture

**[README의 아키텍처 테이블로 화면 전환 또는 터미널에서 cat README.md]**

"The system has four layers:

1. **Data Ingestion** — Events flow in from RSS feeds, Financial Modeling Prep API, FRED economic data, and on-chain signals.

2. **Causal Graph** — SurrealDB stores the cause-effect relationships. Gemini 2.0 analyzes event pairs to discover causal links.

3. **Prediction Pipeline** — The graph is queried for causal chains relevant to each trading pair, generating probabilistic predictions.

4. **Execution** — Predictions are translated into paper trades via Kraken CLI.

A virtual ledger tracks every prediction with hit/miss feedback, continuously refining the causal graph."

---

## [4:00 - 4:30] Closing

"Volva proves that causal AI can generate tradable alpha. The graph grows smarter with each prediction cycle.

The code is fully open source under MIT license at the link below.

Future work includes live trading, on-chain oracle settlement, and multi-asset expansion.

Thank you. Check out the repo and let us know what you think!"

---

## Notes for Recording
- 터미널 폰트 크게 (18pt+)
- `volka_kraken_agent.py` → 실제 파일명은 `volva_kraken_agent.py` (오타 주의!)
- 실수하면 잘라서 이어붙이면 됨
- 배경음악/자막 불필요
- mp4로 저장
