# Volva Arc Testnet Demo

## Agentic Economy on Arc Hackathon (April 20-26, 2026)

MIT License

This demo showcases Volva's causal prediction API monetized via USDC nanopayments on Arc L1 (chain 5042002).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENT / AGENT                              │
│   Requests causal predictions → pays USDC nanopayments          │
│   EIP-191 signed queries for authenticity                        │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              VOLVA PREDICTION API (Mock Engine)                  │
│   HTTP 402 on unpaid requests                                   │
│   Returns EIP-191 signed causal predictions                       │
│   Confidence scores: 55-92%                                      │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              USDC NANOPAYMENT LAYER                              │
│   ~$0.001 per query (sub-cent)                                   │
│   Ethers.js v6 transfers                                         │
│   6-decimal precision (USDC standard)                            │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              ARC TESTNET (Chain 5042002)                         │
│   RPC: https://rpc.testnet.arc.network                           │
│   Explorer: https://testnet.arcscan.app/                         │
│   USDC: 0x3600000000000000000000000000000000000000               │
│   Gas: ~$0.006/tx, sub-second finality                          │
│   ≥50 transactions per demo (hackathon requirement)               │
└─────────────────────────────────────────────────────────────────┘
```

## Requirements

- **Node.js 22+** (ESM modules required)
- **npm** (dependencies: ethers v6)
- **Arc testnet USDC** (for live mode)

## Setup

### 1. Install Dependencies

```bash
cd hackathons/arc/demo
npm install
```

### 2. Get Test USDC (Live Mode Only)

For live testnet transactions:

1. Create a Circle Developer account: https://console.circle.com
2. Generate API Key + Entity Secret
3. Request Arc testnet USDC from: https://faucet.circle.com/
   - Select **Arc Testnet**
   - Receive **10 USDC** (sufficient for ~10,000 nanopayments)

For dry-run mode, no funding is needed.

### 3. Configure Wallet (Live Mode)

Set your mnemonic in environment:

```bash
export WALLET_MNEMONIC="your twelve word mnemonic here"
```

## Usage

### Dry-Run Mode (Offline Simulation)

```bash
./demo.sh --dry-run
# or
node arc_demo.js --dry-run

# With custom transaction count
./demo.sh --dry-run --count 100
```

**Dry-run works without any network connection** - all RPC calls are simulated.

### Live Mode (Real Testnet)

```bash
./demo.sh --live
# or
node arc_demo.js --live
```

⚠️ Requires funded Arc testnet wallet with USDC.

## Output

Each run produces:

1. **Terminal output** - Human-readable progress and summary
2. **JSON log** - `demo_output_{timestamp}.json` with:
   - All transactions (hash, amount, explorer URLs)
   - Signed predictions with EIP-191 signatures
   - Margin analysis
   - Network configuration

## Hackathon Requirements Mapping

| Requirement | Status | Implementation |
|------------|--------|----------------|
| Arc settlement + USDC native gas | ✅ | Chain 5042002, USDC at `0x360000...` |
| Circle Nanopayments | ✅ | $0.001/query via ethers transfer |
| ≥50 on-chain transactions | ✅ | Default 50, configurable via `--count` |
| Per-action pricing ≤$0.01 | ✅ | ~$0.007/action ($0.006 gas + $0.001 payment) |
| Margin analysis | ✅ | Arc vs Ethereum mainnet comparison |
| Live demo URL + GitHub repo | ✅ | This demo |
| Circle Product Feedback | ⚠️ | Submit at lablab.ai |

## Transaction Cost Analysis

### Arc Testnet (This Demo)

| Component | Cost |
|-----------|------|
| Gas per transaction | $0.006 |
| Nanopayment per query | $0.001 |
| **Total per action** | **$0.007** |

### Ethereum Mainnet (Comparison)

| Component | Low Congestion | High Congestion |
|-----------|----------------|-----------------|
| Gas per transaction | $2.00 | $5.00 |
| Nanopayment per query | $0.001 | $0.001 |
| **Total per action** | **$2.001** | **$5.001** |

### Why Arc Enables Nanopayments

At $0.007/action on Arc vs $2-5/action on Ethereum mainnet:

- **99.6-99.9% cost reduction** enables sub-cent pricing
- **New business models**: Per-API monetization, micro-transactions
- **Agent-to-agent payments**: Feasible at sub-cent cost
- **Mass adoption**: Transaction costs no longer a barrier

### Scaling Economics

| Queries/Week | Arc Cost | Ethereum L1 Cost | Savings |
|--------------|----------|------------------|---------|
| 1,000 | $7.00 | $2,001+ | $1,994+ |
| 10,000 | $70.00 | $20,010+ | $19,940+ |
| 100,000 | $700.00 | $200,100+ | $199,400+ |

## Demo Flow

1. **Wallet Setup** - Generate/load HD wallet with EIP-191 signing keys
2. **Prediction Generation** - Mock causal engine produces 50 realistic predictions
3. **Nanopayment Execution** - Each prediction triggers $0.001 USDC transfer
4. **EIP-191 Signing** - Each prediction signed for authenticity
5. **Transaction Logging** - All tx hashes, amounts, explorer URLs captured
6. **Summary Output** - JSON + terminal summary with margin analysis

## Files

| File | Purpose |
|------|---------|
| `arc_demo.js` | Main demo script (ESM, Node.js 22+) |
| `demo.sh` | Orchestration shell script |
| `package.json` | Dependencies (ethers v6) |
| `README.md` | This documentation |

## Technical Details

### USDC Decimals
All USDC amounts use 6-decimal precision per Circle standard:
```
1 USDC = 1,000,000 units
$0.001 USDC = 1,000 units
```

### EIP-191 Signing
Predictions are signed using EIP-191 standard:
```
sign(keccak256("\x19Ethereum Signed Message:\n" + len(message) + message)))
```

This enables:
- Agent authentication without key exposure
- Delegated signing (Agent Passport pattern)
- On-chain verification of prediction authenticity

### Arc Testnet Details

| Parameter | Value |
|-----------|-------|
| Chain ID | 5042002 |
| RPC | https://rpc.testnet.arc.network |
| Explorer | https://testnet.arcscan.app/ |
| USDC | 0x3600000000000000000000000000000000000000 |
| Finality | Sub-second deterministic |

## Support

- **Hackathon**: lablab.ai/ai-hackathons/nano-payments-arc
- **Arc Docs**: https://docs.arc.io/
- **Circle Developer**: https://console.circle.com
