#!/usr/bin/env python3
"""
Live Sepolia batch resolve for Wave1 (wrk-0024).
Creates audits 12-21 with proper deadlines, then resolves each sequentially.

Handles nonce manually to avoid "replacement transaction underpriced" / "nonce too low" errors.
Each step: wait for TX confirmation before proceeding to next nonce.
"""

import json, os, time, datetime, hashlib
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
BROKER_ADDRESS = "0xF0a8701045aFFdad6fC7f0Cb9811ca1bc655Fbb8"
CHAIN_ID = 84532

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ABI_PATH = os.path.join(SCRIPT_DIR, "lethe_escrow_abi.json")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "wave1_launch_markets.json")
ARTIFACT_DIR = os.path.join(SCRIPT_DIR, "batch_results")

# ── Init ───────────────────────────────────────────────────────────────────────
w3 = Web3(Web3.HTTPProvider(RPC_URL))
assert w3.is_connected(), "RPC not connected"
account = Account.from_key(ORACLE_KEY)

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

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_next_nonce():
    return w3.eth.get_transaction_count(account.address)

def wait_for_receipt(tx_hash, timeout=120):
    """Wait for TX receipt, return None on timeout."""
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

def create_and_send_tx(tx_fn, value=0, gas=None):
    """Build, sign, send a tx and wait for receipt. Returns (receipt, error)."""
    nonce = get_next_nonce()
    tx_dict = tx_fn.build_transaction({
        "from": account.address,
        "nonce": nonce,
        "value": value,
        "chainId": CHAIN_ID,
        "gasPrice": w3.eth.gas_price,
    })
    if gas:
        tx_dict["gas"] = gas
    signed = w3.eth.account.sign_transaction(tx_dict, private_key=ORACLE_KEY)
    raw = signed.raw_transaction
    tx_hash = w3.eth.send_raw_transaction(raw)
    print(f"  TX sent: {tx_hash.hex()}")
    receipt = wait_for_receipt(tx_hash)
    if receipt is None:
        return None, "timeout"
    return receipt, None

def compute_poe_hash(market):
    """Build resolution record and compute poeHash."""
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
    """Sign (audit_id, poe_hash) using EIP-191."""
    message_hash = keccak(encode(['uint256', 'bytes32'], [audit_id, poe_hash]))
    encoded_msg = encode_defunct(primitive=message_hash)
    signed = w3.eth.account.sign_message(encoded_msg, private_key=ORACLE_KEY)
    return signed.signature.hex()

# ── Determine audit IDs to use ─────────────────────────────────────────────────
# Current auditCount = 3. We need 10 markets.
# We create 10 new audits (IDs 4-13) so auditCount becomes 13.
# Then resolve audits 4-13.
NUM_MARKETS = len(markets)
START_AUDIT_ID = 4  # Start after existing auditCount=3
END_AUDIT_ID = START_AUDIT_ID + NUM_MARKETS - 1  # = 13

print(f"\nWill create + resolve audits {START_AUDIT_ID}-{END_AUDIT_ID} ({NUM_MARKETS} markets)")

# ── Step 1: Create missing audits ─────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"STEP 1: CREATE AUDITS {START_AUDIT_ID}-{END_AUDIT_ID}")
print(f"{'='*60}")

audit_ids = list(range(START_AUDIT_ID, END_AUDIT_ID + 1))
current_audit_count = escrow.functions.auditCount().call()
print(f"Current auditCount: {current_audit_count}")

# Use a future deadline (~1 year from now)
DEADLINE = int(time.time()) + 365 * 24 * 3600  # 1 year
CODE_HASH = b"\x00" * 32
VALUE_WEI = w3.to_wei("0.01", "ether")

for i, audit_id in enumerate(audit_ids):
    print(f"\n--- Creating Audit {audit_id} (Market {i+1}/{NUM_MARKETS}) ---")
    print(f"  market_id: {markets[i].get('id') or markets[i].get('market_id', 'unknown')}")
    
    # Check if already exists
    try:
        a = escrow.functions.audits(audit_id).call()
        existing_state = a[4]
        existing_deadline = a[3]
        print(f"  Audit {audit_id} already exists: state={existing_state}, deadline={existing_deadline}")
        if existing_deadline > 0:
            print(f"  Skipping createAudit — using existing audit")
            continue
    except Exception as e:
        print(f"  Audit {audit_id} slot check: {e}")
    
    nonce = get_next_nonce()
    gas_price = w3.eth.gas_price
    
    tx_fn = escrow.functions.createAudit(
        to_checksum_address(BROKER_ADDRESS),
        DEADLINE,
        CODE_HASH
    )
    receipt, err = create_and_send_tx(tx_fn, value=VALUE_WEI)
    
    if err or receipt.status != 1:
        reason = "unknown"
        if receipt:
            # Try to decode revert reason
            reason = f"status={receipt.status}"
        print(f"  ❌ createAudit {audit_id} FAILED: {reason}")
        print(f"  TX: https://sepolia.basescan.org/tx/{receipt.transactionHash.hex() if receipt else 'N/A'}")
        # Continue to next
    else:
        gas_used = receipt.gasUsed
        print(f"  ✅ createAudit {audit_id} CONFIRMED (gas: {gas_used})")
        print(f"  https://sepolia.basescan.org/tx/{receipt.transactionHash.hex()}")
    
    time.sleep(3)  # Brief pause between txs

print(f"\nFinal auditCount: {escrow.functions.auditCount().call()}")

# ── Step 2: Resolve each market ────────────────────────────────────────────────
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
        print(f"  On-chain state before: {state_before} (0=Active,1=Resolved,2=Refunded), deadline={deadline}")
    except Exception as e:
        print(f"  ⚠️  Could not check audit state: {e}")
    
    # Resolve
    nonce = get_next_nonce()
    tx_fn = escrow.functions.resolve(audit_id, poe_hash, to_bytes(hexstr=signature))
    receipt, err = create_and_send_tx(tx_fn, value=0)
    
    result = {
        "market_id": market_id,
        "audit_id": audit_id,
        "poe_hash": f"0x{poe_hash.hex()}",
        "signature": signature,
        "record": record,
    }
    
    if err:
        result["status"] = "error"
        result["error"] = err
        result["transaction_hash"] = None
        print(f"  ❌ RESOLVE {audit_id} ERROR: {err}")
    elif receipt.status == 1:
        result["status"] = "success"
        result["transaction_hash"] = receipt.transactionHash.hex()
        result["block_number"] = receipt.blockNumber
        result["gas_used"] = receipt.gasUsed
        result["effective_gas_price"] = receipt.effectiveGasPrice
        result["basescan_url"] = f"https://sepolia.basescan.org/tx/{receipt.transactionHash.hex()}"
        
        # Parse events
        try:
            logs = escrow.events.AuditResolved().process_receipt(receipt)
            result["events"] = [{"auditId": log.args.auditId, "poeHash": log.args.poeHash.hex()} for log in logs]
        except Exception as e:
            result["events"] = f"parse_error: {e}"
        
        print(f"  ✅ RESOLVE {audit_id} SUCCESS")
        print(f"  TX: https://sepolia.basescan.org/tx/{receipt.transactionHash.hex()}")
        print(f"  Block: {receipt.blockNumber}, Gas used: {receipt.gasUsed}")
    else:
        result["status"] = "failed"
        result["transaction_hash"] = receipt.transactionHash.hex()
        result["block_number"] = receipt.blockNumber
        result["gas_used"] = receipt.gasUsed
        result["effective_gas_price"] = receipt.effectiveGasPrice
        result["basescan_url"] = f"https://sepolia.basescan.org/tx/{receipt.transactionHash.hex()}"
        print(f"  ❌ RESOLVE {audit_id} FAILED (reverted on-chain)")
        print(f"  TX: https://sepolia.basescan.org/tx/{receipt.transactionHash.hex()}")
    
    batch_results.append(result)
    time.sleep(5)  # 5s pause between resolves

# ── Save batch results ─────────────────────────────────────────────────────────
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
    "broker": BROKER_ADDRESS,
    "total_markets": len(markets),
    "audit_id_range": [START_AUDIT_ID, END_AUDIT_ID],
    "results": batch_results,
}

with open(results_path, "w") as f:
    json.dump(summary, f, indent=2, default=str)

print(f"\n{'='*60}")
print(f"BATCH COMPLETE")
print(f"{'='*60}")
successes = [r for r in batch_results if r["status"] == "success"]
failures = [r for r in batch_results if r["status"] != "success"]
print(f"Total: {len(batch_results)}")
print(f"Successful: {len(successes)}")
print(f"Failed/Error: {len(failures)}")
for r in batch_results:
    status_str = "✅" if r["status"] == "success" else "❌"
    print(f"  {status_str} audit_id={r['audit_id']} market_id={r['market_id']} status={r['status']}")
    if r.get("basescan_url"):
        print(f"       {r['basescan_url']}")
print(f"\nFull results: {results_path}")

# Save individual market resolution records
for r in batch_results:
    if r["status"] == "success":
        record_path = os.path.join(ARTIFACT_DIR, f"resolution_{r['market_id'].replace(':', '_')}_{r['audit_id']}.json")
        with open(record_path, "w") as f:
            json.dump(r["record"], f, indent=2)
        print(f"Record saved: {record_path}")
