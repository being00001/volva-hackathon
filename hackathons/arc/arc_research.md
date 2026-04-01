# Agentic Economy on Arc — Hackathon Research & Strategy

## Event Summary
- **Platform**: lablab.ai
- **URL**: https://lablab.ai/ai-hackathons/nano-payments-arc
- **Dates**: April 20-26, 2026 (online Apr 20-25, SF finale Apr 25-26)
- **Prize Pool**: $10K+ USDC (Online 1st: $2.5K, 2nd: $1.5K | On-site 1st: $3K + credits, 2nd: $1.5K, 3rd: $1K | Feedback prize: $500)
- **License**: MIT
- **Registration**: lablab.ai account → "Enrol Now" (upopo already has lablab.ai account from Lablab.ai Trading)

## Core Requirements
1. **Mandatory**: Arc settlement + USDC native gas + Circle Nanopayments (sub-cent txns)
2. **≥50 on-chain transactions** in demo
3. **Per-action pricing ≤$0.01**
4. **Margin analysis**: why it fails under traditional gas costs
5. **Live demo URL** + GitHub repo + ≤5min video
6. **Circle Product Feedback** (mandatory, best gets $500)

## Arc Testnet Details
- **Chain ID**: 5042002
- **RPC**: https://rpc.testnet.arc.network
- **Explorer**: https://testnet.arcscan.app/
- **USDC Address**: 0x3600000000000000000000000000000000000000
- **Faucet**: https://faucet.circle.com/ (select Arc Testnet → 10 USDC)
- **Gas**: ~$0.006 USDC/tx, USDC is native gas token
- **Finality**: Sub-second deterministic

## Developer Setup
1. Circle Developer Account (console.circle.com)
2. API Key + Entity Secret
3. Node.js v22+
4. Circle SDK: `@circle-fin/developer-controlled-wallets` + `@circle-fin/smart-contract-platform`
5. SCA (Smart Contract Account) wallets for Gas Station (gasless UX)
6. Alternative: ethers.js direct (no Circle account needed for basic tests)

## Volva Fit Analysis

### Best Track: Track 1 — Per-API Monetization Engine
- Volva's causal prediction API charges USDC per query
- Each prediction = 1 nanopayment ($0.001-$0.01)
- 50+ queries in demo = 50+ on-chain txns ✓

### Secondary Track: Track 2 — Agent-to-Agent Payment Loop
- Volva as oracle → Lethe (or another agent) pays per prediction
- Autonomous payment loop between agents

### Existing Assets to Leverage
1. **x402 mock** (kite_pay_mock.js) → adapt to real Circle Nanopayments API
2. **EIP-191 signing** → works on Arc (EVM-compatible)
3. **Causal prediction engine** → same API, new payment layer
4. **SurrealDB + predictor.py** → unchanged backend
5. **Lablab.ai account** → upopo already registered

### What Needs Building
1. Arc testnet wallet setup (Circle Dev-Controlled Wallets)
2. Payment contract or Circle Nanopayments integration
3. Per-query pricing endpoint ($0.001-$0.01 per prediction)
4. 50+ txn demo script
5. Margin analysis (traditional gas ~$0.50-2.00 vs Arc ~$0.006)
6. Demo video + live URL

### Architecture (Adapted from Kite AI)
```
┌─────────────────────────────────────────────────────┐
│                  CLIENT / BUYER AGENT                │
│  Requests prediction → pays USDC via Nanopayments   │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│              VOLVA PREDICTION API                    │
│  HTTP 402 → X-PAYMENT header → settle → response   │
│  x402 protocol adapted to Circle Nanopayments       │
└──────────────────────┬──────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────┐
│           ARC TESTNET (Chain 5042002)                │
│  USDC settlement per query (~$0.001)                │
│  Circle Gas Station → gasless for buyer             │
│  ≥50 txns in demo → explorer verifiable             │
└─────────────────────────────────────────────────────┘
```

## Timeline
- **Apr 1-10**: Research + Circle account setup + testnet wallet
- **Apr 11-15**: Adapt x402 mock → real Circle Nanopayments
- **Apr 16-19**: 50+ txn demo + margin analysis + video
- **Apr 20-25**: Online build, polish, submit
- **Apr 25**: Submission deadline (online)

## Registration Status
- 🔲 upopo: Register on lablab.ai/ai-hackathons/nano-payments-arc (may auto-enroll with existing account)
- 🔲 Circle Developer Account (console.circle.com)
- 🔲 API Key + Entity Secret generation

## Key Differentiators for Submission
1. **Real AI utility**: Causal prediction is provably valuable (77%+ hit rate)
2. **Per-query economics**: $0.001/query enables new business models impossible on Ethereum
3. **Self-improving**: Feedback loop makes predictions better over time
4. **Verifiable**: Every prediction signed on-chain (EIP-191)
5. **Agent-native**: Designed for machine-to-machine commerce, not human UI
