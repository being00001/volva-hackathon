#!/usr/bin/env python3
"""
Live Sepolia batch resolve for Wave1 (wrk-0024) - FIXED broker signer issue.

The deployed LetheEscrow.resolve() requires the signature signer == broker.
The test pattern (test_on_chain.py) shows broker = oracle_address.
This script creates audits with broker=ORACLE_ADDRESS, then resolves them
signing with ORACLE_KEY (matching the working test pattern).
"""

import json, os, time, datetime
from web3 import Web3
from eth_account import Account
from eth_utils import keccak, to_checksum_address, to_bytes
from eth_abi import encode
from eth_account.messages import encode_defunct

# ── Config ─────────────────────────────────────────────────────────────────────
RPC_URL = "https://sepolia.base.org"
ORACLE_KEY = "70768da9da8a188c5ad302bc04fb57e97e391aa64df060b67030854db9d7ce38"
ORACLE_ADDRESS = "0x9da89c2194ac359CAbaC6DC4548152d885B046a0"
CONTRACT_ADDRESS = "0x1036522C3703971cc3c3c0ead908eAe52c1ba328"
BROKER_ADDRESS = ORACLE_ADDRESS  # ← FIX: use oracle as broker (matches test pattern)
CHAIN_ID = 84532

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ABI_PATH = os.path.join(SCRIPT_DIR, "lethe_escrow_abi.json")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "wave1_launch_markets.json")
ARTIFACT_DIR = os.path.join(SCRIPT_DIR, "batch_results")

# ── Init ───────────────────────────────────────────────────────────────────────
w3 = Web3(Web3.HTTPProvider(RPC_URL))
assert w3.is_connected(), "RPC not connected"
account = Account.from_key(ORACLE_KEY)
assert account.address.lower() == ORACLE_ADDRESS.lower()

with open(ABI_PATH) as f:
    escrow_abi = json.load(f)
escrow = w3.eth.contract(address=to_checksum_address(CONTRACT_ADDRESS), abi=escrow_abi)

with open(CONFIG_PATH) as f:
    config = json.load(f)
markets = config["markets"]

print(f"Oracle: {account.address}")
print(f"Wallet balance: {w3.from_wei(w3.eth.get_balance(account.address), 'ether')} ETH")
print(f"Current nonce: {w3.eth.get_transaction_count(account.address)}")
print(f"auditCount: {escrow.functions.auditCount().call()}")
print(f"Markets: {len(markets)}")
print(f"Broker (for new audits): {BROKER_ADDRESS}")

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_next_nonce():
    return w3.eth.get_transaction_count(account.address)

def wait_for_receipt(tx_hash, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                return receipt
        except Exception:
            pass
        time.sleep(2)
    return None

def build_and_send(tx_fn, value=0, gas=None, gas_price=None):
    """Build, sign, send a tx and wait for receipt. Returns (receipt, error_str)."""
    nonce = get_next_nonce()
    tx_dict = tx_fn.build_transaction({
        "from": account.address,
        "nonce": nonce,
        "value": value,
        "chainId": CHAIN_ID,
        "gasPrice": gas_price or w3.eth.gas_price,
    })
    if gas:
        tx_dict["gas"] = gas
    signed = w3.eth.account.sign_transaction(tx_dict, private_key=ORACLE_KEY)
    raw = signed.raw_transaction
    tx_hash = w3.eth.send_raw_transaction(raw)
    print(f"  TX: {tx_hash.hex()}")
    receipt = wait_for_receipt(tx_hash)
    return receipt, tx_hash.hex()

def compute_poe_hash(market):
    """Build resolution record and compute poeHash (deterministic, sort_keys)."""
    market_id = market.get("id") or market.get("market_id", "unknown")
    resolution_criteria = market.get("resolution_criteria", {})
    record = {
        "id": market_id,
        "title": market.get("title", market.get("question", ""))[:200],
        "rule": resolution_criteria.get("rule", "")[:500],
        "logic": resolution_criteria.get("logic", "")[:1000],
        "outcome": "YES",
        "resolve_time": resolution_criteria.get("resolve_date", datetime.datetime.now(datetime.UTC).isoformat()),
        "sources": ["volva:oracle", "lethe:pusher"],
        "völva_v": "0.4.0-phase4",
        "nonce": f"{time.time_ns()}"
    }
    poe_hash = keccak(text=json.dumps(record, sort_keys=True))
    return poe_hash, record

def sign_resolution(audit_id, poe_hash):
    """
    Sign (audit_id, poe_hash) — matches test_on_chain.py pattern exactly.
    keccak256(abi.encode(auditId, poeHash)) → EIP-191 → sign with oracle key.
    """
    encoded_msg = encode(['uint256', 'bytes32'], [audit_id, poe_hash])
    message_hash = keccak(encoded_msg)
    msg = encode_defunct(hexstr=message_hash.hex())
    signed = Account.sign_message(msg, ORACLE_KEY)
    return signed.signature.hex()

# ── Determine audit IDs ─────────────────────────────────────────────────────────
current_count = escrow.functions.auditCount().call()
NUM_MARKETS = len(markets)
START_AUDIT_ID = current_count + 1  # Start after existing audits
END_AUDIT_ID = START_AUDIT_ID + NUM_MARKETS - 1

print(f"\nExisting auditCount: {current_count}")
print(f"Will create + resolve audits {START_AUDIT_ID}-{END_AUDIT_ID} ({NUM_MARKETS} markets)")

# Use a future deadline (~1 year from now)
DEADLINE = int(time.time()) + 365 * 24 * 3600
CODE_HASH = b"\x00" * 32
VALUE_WEI = w3.to_wei("0.01", "ether")

audit_ids = list(range(START_AUDIT_ID, END_AUDIT_ID + 1))

# ── Step 1: Create audits ──────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"STEP 1: CREATE AUDITS {START_AUDIT_ID}-{END_AUDIT_ID}")
print(f"{'='*60}")

for i, audit_id in enumerate(audit_ids):
    market = markets[i]
    market_id = market.get("id") or market.get("market_id", "unknown")
    print(f"\n--- Creating Audit {audit_id} (Market {i+1}/{NUM_MARKETS}): {market_id} ---")

    tx_fn = escrow.functions.createAudit(
        to_checksum_address(BROKER_ADDRESS),  # broker = oracle (FIX!)
        DEADLINE,
        CODE_HASH
    )
    receipt, txh = build_and_send(tx_fn, value=VALUE_WEI)

    if receipt is None:
        print(f"  ❌ TIMEOUT creating audit {audit_id}")
    elif receipt.status == 1:
        # Parse auditId from event
        try:
            logs = escrow.events.AuditCreated().process_receipt(receipt)
            created_aid = logs[0].args.auditId
            print(f"  ✅ CONFIRMED (gas: {receipt.gasUsed}) — auditId from event: {created_aid}")
        except Exception as e:
            print(f"  ✅ CONFIRMED (gas: {receipt.gasUsed}) — event parse error: {e}")
        print(f"  https://sepolia.basescan.org/tx/{txh}")
    else:
        print(f"  ❌ FAILED (reverted) — https://sepolia.basescan.org/tx/{txh}")

    time.sleep(3)

print(f"\nFinal auditCount: {escrow.functions.auditCount().call()}")

# ── Step 2: Resolve ────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"STEP 2: RESOLVE AUDITS {START_AUDIT_ID}-{END_AUDIT_ID}")
print(f"{'='*60}")

batch_results = []

for i, audit_id in enumerate(audit_ids):
    market = markets[i]
    market_id = market.get("id") or market.get("market_id", "unknown")
    print(f"\n--- Resolving Market {i+1}/{NUM_MARKETS}: {market_id} ---")
    print(f"  Audit ID: {audit_id}")

    # Compute poeHash and sign
    poe_hash, record = compute_poe_hash(market)
    signature = sign_resolution(audit_id, poe_hash)

    print(f"  poeHash: 0x{poe_hash.hex()}")
    print(f"  Signature: {signature[:40]}...")

    # Check on-chain state before resolve
    try:
        a = escrow.functions.audits(audit_id).call()
        state_before = a[4]
        deadline = a[3]
        broker_in_audit = a[1]
        print(f"  On-chain: state={state_before}, deadline={deadline}, broker={broker_in_audit}")
    except Exception as e:
        print(f"  ⚠️  Audit check failed: {e}")

    # Resolve
    tx_fn = escrow.functions.resolve(audit_id, poe_hash, to_bytes(hexstr=signature))
    receipt, txh = build_and_send(tx_fn, value=0)

    result = {
        "market_id": market_id,
        "audit_id": audit_id,
        "poe_hash": f"0x{poe_hash.hex()}",
        "signature": signature,
        "record": record,
        "broker": BROKER_ADDRESS,
    }

    if receipt is None:
        result["status"] = "error"
        result["error"] = "tx_timeout"
        result["transaction_hash"] = None
        print(f"  ❌ RESOLVE {audit_id} TIMEOUT")
    elif receipt.status == 1:
        result["status"] = "success"
        result["transaction_hash"] = txh
        result["block_number"] = receipt.blockNumber
        result["gas_used"] = receipt.gasUsed
        result["effective_gas_price"] = receipt.effectiveGasPrice
        result["basescan_url"] = f"https://sepolia.basescan.org/tx/{txh}"

        # Parse events
        try:
            logs = escrow.events.AuditResolved().process_receipt(receipt)
            result["events"] = [{"auditId": log.args.auditId, "poeHash": log.args.poeHash.hex()} for log in logs]
        except Exception as e:
            result["events"] = f"parse_error: {e}"

        print(f"  ✅ RESOLVE {audit_id} SUCCESS — block={receipt.blockNumber}, gas={receipt.gasUsed}")
        print(f"  https://sepolia.basescan.org/tx/{txh}")
    else:
        result["status"] = "failed"
        result["transaction_hash"] = txh
        result["block_number"] = receipt.blockNumber
        result["gas_used"] = receipt.gasUsed
        result["effective_gas_price"] = receipt.effectiveGasPrice
        result["basescan_url"] = f"https://sepolia.basescan.org/tx/{txh}"
        print(f"  ❌ RESOLVE {audit_id} REVERTED — https://sepolia.basescan.org/tx/{txh}")

    batch_results.append(result)
    time.sleep(5)

# ── Save results ───────────────────────────────────────────────────────────────
os.makedirs(ARTIFACT_DIR, exist_ok=True)
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
results_path = os.path.join(ARTIFACT_DIR, f"live_batch_{ts}.json")

summary = {
    "schema": "lethe_batch_live_results_v1",
    "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
    "network": "base_sepolia",
    "wave": "wave1",
    "phase": "phase7_e2e",
    "worker": "wrk-0024",
    "oracle": ORACLE_ADDRESS,
    "escrow": CONTRACT_ADDRESS,
    "broker_used": BROKER_ADDRESS,
    "total_markets": len(markets),
    "audit_id_range": [START_AUDIT_ID, END_AUDIT_ID],
    "deadline_used": DEADLINE,
    "results": batch_results,
}

with open(results_path, "w") as f:
    json.dump(summary, f, indent=2, default=str)

# Save individual records
for r in batch_results:
    if r["status"] == "success":
        record_path = os.path.join(ARTIFACT_DIR, f"resolution_{r['market_id'].replace(':', '_')}_{r['audit_id']}.json")
        with open(record_path, "w") as f:
            json.dump(r["record"], f, indent=2)

# ── Summary ─────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"BATCH COMPLETE")
print(f"{'='*60}")
successes = [r for r in batch_results if r["status"] == "success"]
failures = [r for r in batch_results if r["status"] != "success"]
print(f"Total: {len(batch_results)}")
print(f"Successful: {len(successes)}")
print(f"Failed/Error: {len(failures)}")
for r in batch_results:
    icon = "✅" if r["status"] == "success" else "❌"
    print(f"  {icon} audit_id={r['audit_id']} market_id={r['market_id']} status={r['status']}")
    if r.get("basescan_url"):
        print(f"      {r['basescan_url']}")
print(f"\nFull results: {results_path}")
print(f"Wallet balance after: {w3.from_wei(w3.eth.get_balance(account.address), 'ether')} ETH")
