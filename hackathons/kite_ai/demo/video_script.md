# Volva Kite AI Demo - Video Script

## Title
**Volva: Causal Prediction Oracle for Agentic Trading on Kite Chain**

## Duration
3 minutes (180 seconds)

## Target Audience
Kite AI Hackathon judges, Lablab.ai community

---

## SCENE 1: Hook (0:00 - 0:20)

**[CUT TO: Terminal with causal graph visualization]**

**NARRATOR:**
> "Trading agents react to price moves. But what if an agent could predict *before* the news breaks?"
> 
> "That's what Volva does. A causal graph engine that traces cause-and-effect chains between macro events and market outcomes."

**[SHOW: Terminal showing causal graph query]**

---

## SCENE 2: The Problem (0:20 - 0:45)

**[CUT TO: Dashboard showing market data feeds]**

**NARRATOR:**
> "Current AI trading agents are reactive. They see the price move, then they react."
> 
> "No agent proactively traces: Fed decisions → interest rates → risk sentiment → asset prices."
> 
> "This leaves alpha on the table."

**[SHOW: Data sources - RSS, FMP, FRED, Grok APIs]**

---

## SCENE 3: The Solution - Causal Graph (0:45 - 1:15)

**[CUT TO: SurrealDB causal graph visualization]**

**NARRATOR:**
> "Volva builds a real-time causal graph from multi-source data."
> 
> "Today: 1,869 events, 39 causal links, 2.09% density."
> 
> "Our Gemini 2.5 reasoning engine generates probabilistic predictions with 77%+ validated hit rate."

**[SHOW: Live prediction generation]**

> "Example prediction: 'Fed rate decision → risk-on → BTC bullish (0.82 confidence)'"

---

## SCENE 4: Kite AI Integration (1:15 - 2:00)

**[CUT TO: Demo terminal showing Agent Passport + AA wallet]**

**NARRATOR:**
> "Now let's show how Volva uses Kite AI's infrastructure for trustless execution."
> 
> "First, Agent Passport: every prediction signed with EIP-191 → verifiable on-chain attestation."

**[SHOW: Terminal output with delegation signature]**

> "HD wallet derived session key signs the delegation. No manual key management."

**[SHOW: AA wallet creation]**

> "Second, Account Abstraction: Volva operates with zero ETH."
> 
> "Gasless transactions via Kite's bundler. The agent never needs gas money."

**[SHOW: UserOperation submission]**

> "Third, kite.pay() monetization: premium signals settled in USDC via x402."

**[SHOW: HTTP 402 + X-PAYMENT header]**

> "User requests premium signal → server returns 402 with payment header → facilitator settles."

---

## SCENE 5: E2E Demo (2:00 - 2:30)

**[CUT TO: Full demo flow running]**

**NARRATOR:**
> "Let's run the full demo on Kite testnet, chain 2368."
> 
> "1. Agent Passport delegation signed"
> 
> "2. AA wallet deployed"
> 
> "3. Faucet KITE and USDC received"
> 
> "4. Portfolio rebalancing: approve + transfer batch"
> 
> "5. Gasless UserOperation submitted via bundler"

**[SHOW: Transaction hash + explorer link]**

> "All verifiable on Kite testnet explorer."

---

## SCENE 6: What Makes This Novel (2:30 - 2:50)

**[CUT TO: Architecture diagram]**

**NARRATOR:**
> "Four things make Volva unique:"
> 
> "1. Proactive, not reactive: causal graphs predict *before* news breaks"
> 
> "2. Trustless verification: every prediction signed on-chain"
> 
> "3. Self-improving: feedback loop tightens the graph with each cycle"
> 
> "4. Zero-human: fully autonomous from ingestion to settlement"

---

## SCENE 7: Call to Action (2:50 - 3:00)

**[CUT TO: Links + GitHub repo]**

**NARRATOR:**
> "GitHub: github.com/being00001/volva (MIT license)"
> 
> "Kite AI Hackathon submission - April 6 outline, April 26 final."
> 
> "Thank you."

**[END CARD: Volva + Kite AI logos]**

---

## Technical Notes for Editor

### Required Screenshots
1. Causal graph query result (live terminal)
2. Agent Passport delegation signature
3. AA wallet address + explorer confirmation
4. UserOperation submission tx hash
5. kite.pay() 402 response
6. Explorer verification link

### B-Roll Needed
- Terminal with green text on dark background
- Network graph visualization
- API response animations
- Explorer page showing transaction

### Audio
- Background: Subtle electronic/ambient music (royalty-free)
- Tones: Confirmation beeps for successful operations
- Voice: Clear, confident, technical but accessible

### Captions
- Include for all API endpoints and transaction hashes
- Highlight key numbers: 77% hit rate, 1,869 events, 39 links

---

## Resources

- **Demo repo**: `hackathons/kite_ai/demo/`
- **Explorer**: https://explorer.kiteai.net/
- **Faucet**: https://faucet.gokite.ai/
- **RPC**: https://rpc-testnet.gokite.ai (Chain 2368)
- **Bundler**: https://bundler-service.staging.gokite.ai/rpc/
