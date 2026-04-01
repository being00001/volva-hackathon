# Volva Kite AI Demo

## Kite AI Hackathon - Agentic Trading Demo

This directory contains the Volva agentic trading demo for the Kite AI hackathon (qn-e7e21581).

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Volva Agent                           │
│  Causal Graph → Predictions → EIP-191 Attestations     │
└─────────────────────┬───────────────────────────────────┘
                      │ Agent Passport (EIP-191)
                      ▼
┌─────────────────────────────────────────────────────────┐
│              Kite AI Layer (Chain 2368)                │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │  Agent Passport │  │     Account Abstraction     │  │
│  │  Identity Layer  │  │     (gokite-aa-sdk)         │  │
│  └─────────────────┘  └─────────────────────────────┘  │
│  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │   kite.pay()    │  │      Gasless TXs            │  │
│  │   (x402 USDC)   │  │  (No ETH required)         │  │
│  └─────────────────┘  └─────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
              ┌───────────────────┐
              │  Kite Testnet     │
              │  Chain ID: 2368   │
              │  RPC: rpc-testnet │
              │  .gokite.ai       │
              └───────────────────┘
```

### Quick Start

```bash
# Install dependencies
npm install

# Run dry-run demo (no real transactions)
npm run demo:dry-run

# Start the kite.pay mock server (separate terminal)
npm run mock:server

# Full E2E demo (requires faucet funds)
npm run demo

# Run E2E orchestration script
bash demo.sh
```

### Demo Flow

1. **Agent Passport**: Generate EIP-191 signed delegation using HD wallet derived key
2. **AA Wallet**: Create smart contract wallet on Kite testnet
3. **Faucet**: Request KITE/USDC test tokens (via https://faucet.gokite.ai)
4. **Gasless TX**: Send UserOperation via bundler - approve + transfer batch
5. **kite.pay()**: Simulate x402 payment for premium signal subscription

### Network Configuration

| Network    | Chain ID | RPC URL                        | Bundler URL                              |
|------------|----------|--------------------------------|------------------------------------------|
| Kite Testnet | 2368     | https://rpc-testnet.gokite.ai | https://bundler-service.staging.gokite.ai/rpc/ |

### Files

| File            | Description                                      |
|-----------------|--------------------------------------------------|
| `demo.js`       | Main AA demo - wallet creation + gasless txs    |
| `kite_pay_mock.js` | Mock x402 payment server                     |
| `demo.sh`       | E2E orchestration script                         |
| `video_script.md` | Video narration script for submission          |

### Explorer

- **Kite Testnet Explorer**: https://explorer.kite.ai/
- View transactions by tx hash after running demo

### License

MIT
