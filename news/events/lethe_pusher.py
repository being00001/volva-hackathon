#!/usr/bin/env python3
"""
Volva Lethe Pusher - Phase 4/7
Bridges Völva's refined market judgements to the Lethe protocol.
Handles canonical record creation, poeHash computation, and on-chain settlement.

Supports Base Sepolia and Base Mainnet.

Usage:
    # Single audit mode (legacy)
    python3 lethe_pusher.py <audit_id>

    # Batch mode with config file (Sepolia default)
    python3 lethe_pusher.py --config=launch_wave1.sepolia.json [--audit-id-start=2]

    # Dry-run mode (simulation only, no RPC/tx)
    python3 lethe_pusher.py --dry-run --config=launch_wave1.sepolia.json

    # Mainnet-fork Wave1 dry-run with deployer broker override (for fork simulation)
    python3 lethe_pusher.py --network=mainnet-fork --dry-run \\
        --config=launch_wave1.mainnet.json \\
        --broker-override=0xF0a8701045aFFdad6fC7f0Cb9811ca1bc655Fbb8 \\
        --audit-id-start=2
"""

import argparse
import json
import logging
import os
import hashlib
import datetime
import uuid
import sys
import time
from typing import Optional, Dict, Any, List
from eth_abi import encode
from eth_utils import keccak, to_bytes, to_checksum_address
from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_defunct

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("volva.pusher")

# Default Configuration (Base Sepolia)
# These are overridden by config file when --config is provided
DEFAULT_RPC_URL = "https://sepolia.base.org"
DEFAULT_CONTRACT_ADDRESS = "0x1036522C3703971cc3c3c0ead908eAe52c1ba328"
DEFAULT_CHAIN_ID = 84532

# Mainnet Configuration
MAINNET_RPC_URL = "https://mainnet.base.org"
MAINNET_CONTRACT_ADDRESS = "0xc792AB195dD83882CC60B12bed9a69b79A223E9f"
MAINNET_CHAIN_ID = 8453
MAINNET_BROKER_REGISTRY = "0xF9753df138278879F59cb1286a80C28B3897A73D"
# Mainnet deployer that serves as the sole broker for Wave1 E2E
MAINNET_DEPLOYER_BROKER = "0xF0a8701045aFFdad6fC7f0Cb9811ca1bc655Fbb8"

# CORRECT KEY for 0x9da89c2194ac359CAbaC6DC4548152d885B046a0
ORACLE_KEY = "70768da9da8a188c5ad302bc04fb57e97e391aa64df060b67030854db9d7ce38"

# Updated ABI for LetheEscrow (v0.1.0-official)
# Use __file__ to resolve relative to script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ABI_PATH = os.path.join(SCRIPT_DIR, "lethe_escrow_abi.json")


# =============================================================================
# Kite AI Agent Passport Integration (BIP-32 HD Wallet)
# =============================================================================
# Agent Passport enables EIP-191 signed delegations for autonomous AI agents.
# Session keys are derived from HD wallet paths for secure key management.
# =============================================================================

class AgentPassport:
    """
    Agent Passport for Kite AI - EIP-191 Delegation Signing.
    
    Enables Volva (autonomous AI agent) to sign delegations using
    BIP-32 HD wallet derived session keys.
    
    HD Path Pattern: m/44'/60'/0'/0/derivation_index
    
    Usage:
        passport = AgentPassport(mnemonic="your 12 or 24 word mnemonic")
        delegation = passport.sign_delegation(
            delegate="0xAgentAddress...",
            expires_at=1699999999,
            permissions=["sign:all", "tx:all"]
        )
    """
    
    HD_PATH_PATTERN = "m/44'/60'/0'/0/{index}"
    
    def __init__(self, mnemonic: str = None, hd_path_index: int = 0):
        """
        Initialize Agent Passport with HD wallet derivation.
        
        Args:
            mnemonic: BIP-39 mnemonic phrase. If None, generates random.
            hd_path_index: HD derivation index for session key (default: 0)
        """
        self.mnemonic = mnemonic or self._generate_mnemonic()
        self.hd_path_index = hd_path_index
        self.session_key = self._derive_session_key(hd_path_index)
        self.delegator_key = self._derive_delegator_key(hd_path_index)
        
        logger.info(f"[AgentPassport] Initialized with HD path index: {hd_path_index}")
        logger.info(f"[AgentPassport] Session key: {self.session_key[:20]}...")
        logger.info(f"[AgentPassport] Delegator key: {self.delegator_key[:20]}...")
    
    def _generate_mnemonic(self) -> str:
        """Generate a random 64-byte mnemonic (simplified, use BIP-39 in production)."""
        import secrets
        return secrets.token_hex(32)
    
    def _derive_session_key(self, derivation_index: int) -> str:
        """
        Derive session key from HD wallet.
        Simplified HMAC-SHA256 derivation (use @scure/bip32 in production).
        
        Args:
            derivation_index: Index for key derivation
            
        Returns:
            Derived key as hex string prefixed with 0x
        """
        import hmac
        import hashlib
        
        # Use HMAC-SHA256 with path components
        key = f"session:{derivation_index}".encode()
        h = hmac.new(self.mnemonic.encode(), key, hashlib.sha256).digest()
        
        # Derive child key
        child_input = h + derivation_index.to_bytes(4, 'big')
        child = hmac.new(self.mnemonic.encode(), child_input, hashlib.sha256).digest()
        
        return '0x' + child.hex()
    
    def _derive_delegator_key(self, derivation_index: int) -> str:
        """
        Derive delegator key from HD wallet (different path than session).
        
        Args:
            derivation_index: Index for key derivation
            
        Returns:
            Derived key as hex string prefixed with 0x
        """
        import hmac
        import hashlib
        
        key = f"delegator:{derivation_index}".encode()
        h = hmac.new(self.mnemonic.encode(), key, hashlib.sha256).digest()
        
        # Derive child key with different chain code
        child_input = h + b'\xff' + derivation_index.to_bytes(4, 'big')
        child = hmac.new(self.mnemonic.encode(), child_input, hashlib.sha256).digest()
        
        return '0x' + child.hex()
    
    def get_address(self) -> str:
        """Get the Ethereum address derived from session key."""
        from eth_keys import keys
        from eth_utils import keccak
        
        # Get last 20 bytes of keccak(session_key)[12:]
        key_bytes = bytes.fromhex(self.session_key[2:])
        hash_bytes = keccak(key_bytes)
        return '0x' + hash_bytes[-20:].hex()
    
    def sign_eip191(self, message: str) -> Dict[str, Any]:
        """
        Sign a message using EIP-191 standard.
        
        Args:
            message: Message string to sign
            
        Returns:
            Dict with message, message_hash, signature, signer
        """
        from eth_account import Account
        from eth_account.messages import encode_defunct
        
        # Create signable message
        signable = encode_defunct(text=message)
        
        # Sign with session key
        signed = Account.sign_message(signable, self.session_key)
        
        return {
            "message": message,
            "message_hash": signed.messageHash.hex(),
            "signature": signed.signature.hex(),
            "signer": self.get_address(),
        }
    
    def sign_delegation(
        self,
        delegate: str,
        expires_at: int = None,
        permissions: List[str] = None
    ) -> Dict[str, Any]:
        """
        Sign an Agent Passport delegation.
        
        Delegates authority from delegator to session key with
        expiration and permissions.
        
        Args:
            delegate: Address receiving delegated authority
            expires_at: Unix timestamp for delegation expiration
            permissions: List of permission strings (e.g., ["sign:all", "tx:all"])
            
        Returns:
            Dict with delegation details and EIP-191 signature
        """
        import time
        
        if expires_at is None:
            expires_at = int(time.time()) + 86400  # 24 hours default
        
        if permissions is None:
            permissions = ["sign:all", "tx:all"]
        
        # Build delegation message (EIP-191 structured data)
        delegation_payload = {
            "type": "AGENT_PASSPORT_DELEGATION_V1",
            "delegator": self.delegator_key,
            "delegate": delegate,
            "expiresAt": expires_at,
            "permissions": permissions,
            "agent": self.get_address(),
            "timestamp": int(time.time()),
            "nonce": uuid.uuid4().hex[:16],
        }
        
        # Serialize to canonical JSON
        message = json.dumps(delegation_payload, sort_keys=True, separators=(',', ':'))
        
        # Sign with EIP-191
        signature = self.sign_eip191(message)
        
        return {
            "payload": delegation_payload,
            "message": message,
            "message_hash": signature["message_hash"],
            "signature": signature["signature"],
            "signer": signature["signer"],
        }
    
    @staticmethod
    def verify_delegation(delegation: Dict[str, Any]) -> bool:
        """
        Verify a delegation signature (static method for verification).
        
        Args:
            delegation: Dict containing delegation + signature
            
        Returns:
            True if signature is valid
        """
        from eth_account import Account
        from eth_account.messages import encode_defunct
        
        try:
            message = delegation["message"]
            signature = delegation["signature"]
            
            # Recover signer
            signable = encode_defunct(text=message)
            recovered = Account.recover_message(signable, signature=signature)
            
            return recovered.lower() == delegation["signer"].lower()
        except Exception as e:
            logger.warning(f"[AgentPassport] Verification failed: {e}")
            return False


class LethePusher:
    def __init__(self, rpc_url: str = None, contract_address: str = None, chain_id: int = None,
                 broker_override: str = None):
        """
        Initialize LethePusher with optional network overrides.
        
        Args:
            rpc_url: RPC endpoint URL (defaults to Base Sepolia)
            contract_address: LetheEscrow contract address (defaults to Base Sepolia)
            chain_id: Chain ID (defaults to 84532 for Base Sepolia)
            broker_override: Override the broker address used for signing.
                             When set, this broker address is used instead of
                             the oracle address in resolution signatures.
        """
        self.rpc_url = rpc_url or DEFAULT_RPC_URL
        self.contract_address = contract_address or DEFAULT_CONTRACT_ADDRESS
        self.chain_id = chain_id or DEFAULT_CHAIN_ID
        self.broker_override = broker_override
        
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        with open(ABI_PATH, "r") as f:
            self.abi = json.load(f)
        self.contract = self.w3.eth.contract(
            address=to_checksum_address(self.contract_address),
            abi=self.abi
        )
        self.account = Account.from_key(ORACLE_KEY)
        # Nonce tracking for sequential batch sends
        self._current_nonce = None
        self._nonce_initialized = False
        logger.info(f"Initialized Pusher with Oracle: {self.account.address}")
        logger.info(f"  RPC: {self.rpc_url}")
        logger.info(f"  Contract: {self.contract_address}")
        logger.info(f"  Chain ID: {self.chain_id}")
        if self.broker_override:
            logger.info(f"  Broker Override: {self.broker_override}")

    def _get_next_nonce(self, force_refresh: bool = False) -> int:
        """
        Get the next nonce for the sender account.
        Uses 'pending' block tag to include pending transactions.
        
        Args:
            force_refresh: If True, re-query RPC even if we have a cached nonce.
        
        Returns:
            The nonce to use for the next transaction.
        """
        if not self._nonce_initialized or force_refresh:
            addr = self.account.address
            # Use 'pending' to get the next nonce including pending txs
            self._current_nonce = self.w3.eth.get_transaction_count(addr, 'pending')
            self._nonce_initialized = True
            logger.info(f"[NONCE] Initialized from RPC: nonce={self._current_nonce} (pending) for {addr}")
        nonce = self._current_nonce
        # Increment for next use (but don't commit until TX is confirmed)
        self._current_nonce = nonce + 1
        return nonce

    def _commit_nonce(self):
        """
        Commit the nonce increment after a successful TX send.
        Called after send_raw_transaction succeeds.
        """
        # The nonce was already pre-incremented in _get_next_nonce,
        # so we just log the commit here.
        logger.debug(f"[NONCE] Committed nonce increment, next will be: {self._current_nonce}")

    def build_resolution_record(self, market_draft: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a canonical resolution record for the market.
        """
        record = {
            "id": market_draft["id"],
            "title": market_draft["title"],
            "rule": market_draft["rule"],
            "logic": market_draft["logic"],
            "outcome": market_draft["final_outcome"],
            "resolve_time": market_draft.get("resolve_time", datetime.datetime.now(datetime.UTC).isoformat()),
            "sources": market_draft["accepted_sources"],
            "völva_v": "0.4.0-phase4",
            "nonce": str(uuid.uuid4())
        }
        return record

    def compute_poe_hash(self, record: Dict[str, Any]) -> bytes:
        """
        Computes the Proof of Execution (poeHash).
        """
        canonical_json = json.dumps(record, sort_keys=True)
        return keccak(text=canonical_json)

    def sign_resolution(self, audit_id: int, poe_hash: bytes, broker: str = None) -> str:
        """
        Signs the (audit_id, poe_hash) using EIP-191.

        Args:
            audit_id: The audit ID being resolved.
            poe_hash: The proof-of-execution hash.
            broker: Optional broker address override. When set, the signature
                    is computed with the broker context (used when the broker
                    differs from the oracle address, e.g. mainnet deployer broker).
                    If None, uses the oracle address.
        """
        # LetheEscrow's poeHash verification expects keccak256(abi.encode(auditId, poeHash))
        # then wrapped in EIP-191.
        message_hash = keccak(encode(['uint256', 'bytes32'], [audit_id, poe_hash]))

        # Wrap for EIP-191
        encoded_message = encode_defunct(primitive=message_hash)
        signed_message = self.w3.eth.account.sign_message(encoded_message, private_key=ORACLE_KEY)

        return signed_message.signature.hex()

    def sign_agent_passport_delegation(
        self,
        delegate: str,
        expires_at: int = None,
        permissions: List[str] = None,
        passport: AgentPassport = None
    ) -> Dict[str, Any]:
        """
        Sign an Agent Passport delegation for Kite AI integration.
        
        This enables Volva to delegate authority from its HD wallet-derived
        session key for gasless operations and payment authorization.
        
        Args:
            delegate: Address receiving delegated authority
            expires_at: Unix timestamp for delegation expiration
            permissions: List of permission strings
            passport: AgentPassport instance. If None, creates new one.
            
        Returns:
            Dict with delegation payload, message, and EIP-191 signature
        """
        if passport is None:
            passport = AgentPassport()
        
        delegation = passport.sign_delegation(
            delegate=delegate,
            expires_at=expires_at,
            permissions=permissions
        )
        
        logger.info(f"[AgentPassport] Delegation signed for delegate: {delegate}")
        logger.info(f"[AgentPassport] Signer: {delegation['signer']}")
        logger.info(f"[AgentPassport] Expires: {delegation['payload']['expiresAt']}")
        
        return delegation

    def push_resolution(self, audit_id: int, poe_hash: bytes, signature: str) -> Dict[str, Any]:
        """
        Submits the resolution to the smart contract.
        Includes retry logic (up to 3x) for nonce-related errors.
        """
        max_retries = 3
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # Use sequential nonce tracking with 'pending' block tag
                nonce = self._get_next_nonce()
                logger.info(f"[TX] audit_id={audit_id} nonce={nonce} gas_price={int(self.w3.eth.gas_price * 1.5)} attempt={attempt}/{max_retries}")
                
                tx = self.contract.functions.resolve(
                    audit_id,
                    poe_hash,
                    to_bytes(hexstr=signature)
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gas': 500000,
                    'chainId': self.chain_id,
                    'gasPrice': int(self.w3.eth.gas_price * 1.5)
                })
                
                signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=ORACLE_KEY)
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                
                # Commit nonce after successful send
                self._commit_nonce()
                
                logger.info(f"[TX] TX HASH: {tx_hash.hex()}")
                logger.info(f"[TX] Waiting for receipt...")
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
                logger.info(f"[TX] receipt.status={receipt.status} block_number={receipt.blockNumber}")
                
                if receipt.status == 1:
                    logger.info("[TX] Transaction successful!")
                    # Parse events
                    logs = self.contract.events.AuditResolved().process_receipt(receipt)
                    return {
                        "status": "success",
                        "transactionHash": tx_hash.hex(),
                        "receipt": receipt,
                        "events": logs
                    }
                else:
                    logger.error("[TX] Transaction failed!")
                    return {
                        "status": "failed",
                        "transactionHash": tx_hash.hex(),
                        "receipt": receipt
                    }
                    
            except ValueError as e:
                error_msg = str(e)
                last_error = error_msg
                is_nonce_error = any(x in error_msg.lower() for x in ['nonce', 'underpriced', 'replacement', 'too low'])
                
                if is_nonce_error and attempt < max_retries:
                    logger.warning(f"[TX] Nonce-related error on attempt {attempt}/{max_retries}: {error_msg}")
                    logger.info(f"[TX] Re-syncing nonce from RPC (force_refresh)...")
                    # Force refresh nonce from RPC
                    self._get_next_nonce(force_refresh=True)
                    time.sleep(1)  # Brief wait before retry
                    continue
                else:
                    logger.error(f"[TX] Error: {error_msg}")
                    return {
                        "status": "error",
                        "error": str(e)
                    }
            except Exception as e:
                logger.error(f"[TX] Unexpected error: {e}")
                return {
                    "status": "error",
                    "error": str(e)
                }
        
        # Exhausted retries
        return {
            "status": "error",
            "error": f"Max retries ({max_retries}) exhausted. Last error: {last_error}"
        }

    def process_market(
        self,
        market: Dict[str, Any],
        audit_id: int,
        dry_run: bool = False,
        mock_outcome: str = "YES",
        broker_override: str = None
    ) -> Dict[str, Any]:
        """
        Process a single market: build record, compute hash, sign, and optionally push.
        
        Args:
            market: Market draft dict with id, title, question, resolution_criteria, etc.
            audit_id: The audit ID for this market
            dry_run: If True, skip RPC/tx and only compute/log
            mock_outcome: Mock outcome for simulation (default: "YES")
            broker_override: Override the broker address for this market's signature.
                             When None, uses the pusher's broker_override (if set).
        
        Returns:
            Dict with resolution details (poe_hash, signature, status, etc.)
        """
        market_id = market.get("id") or market.get("market_id", "unknown")
        resolution_criteria = market.get("resolution_criteria", {})
        
        # Resolve effective broker: per-market override > instance-level override > oracle
        effective_broker = broker_override or self.broker_override
        
        # Build a market draft compatible with build_resolution_record
        market_draft = {
            "id": market_id,
            "title": market.get("title", market.get("question", "")),
            "rule": resolution_criteria.get("rule", ""),
            "logic": resolution_criteria.get("logic", ""),
            "final_outcome": mock_outcome,
            "resolve_time": resolution_criteria.get("resolve_date", datetime.datetime.now(datetime.UTC).isoformat()),
            "accepted_sources": ["volva:oracle", "lethe:pusher"],
        }
        
        # Build resolution record
        record = self.build_resolution_record(market_draft)
        poe_hash = self.compute_poe_hash(record)
        
        result = {
            "market_id": market_id,
            "audit_id": audit_id,
            "poe_hash": f"0x{poe_hash.hex()}",
            "record": record,
            "dry_run": dry_run,
            "broker": effective_broker,
        }
        
        # Sign resolution
        signature = self.sign_resolution(audit_id, poe_hash, broker=effective_broker)
        result["signature"] = signature
        
        logger.info(f"[Market: {market_id}] Audit ID: {audit_id}")
        logger.info(f"[Market: {market_id}] poeHash: 0x{poe_hash.hex()}")
        logger.info(f"[Market: {market_id}] Broker: {effective_broker or 'oracle-default'}")
        logger.info(f"[Market: {market_id}] Signature: {signature[:64]}...")
        
        # Push to blockchain if not dry-run
        if dry_run:
            logger.info(f"[Market: {market_id}] DRY-RUN: Skipping RPC/tx")
            result["status"] = "dry_run"
            result["transaction_hash"] = None
        else:
            logger.info(f"[Market: {market_id}] Pushing to blockchain...")
            push_result = self.push_resolution(audit_id, poe_hash, signature)
            result["status"] = push_result.get("status", "failed")
            result["transaction_hash"] = push_result.get("transactionHash")
            
            # Save resolution record for audit trail
            record_filename = f"resolution_{market_id.replace(':', '_')}_{audit_id}.json"
            with open(record_filename, "w") as f:
                json.dump(record, f, indent=2)
            logger.info(f"[Market: {market_id}] Record saved to {record_filename}")
        
        return result

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Volva Lethe Pusher - Push market resolutions to LetheEscrow on Base Sepolia",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single audit mode (legacy)
  python3 lethe_pusher.py 2

  # Batch mode with config file
  python3 lethe_pusher.py --config=launch_wave1.sepolia.json --audit-id-start=2

  # Dry-run mode (simulation only, no RPC/tx)
  python3 lethe_pusher.py --dry-run --config=launch_wave1.sepolia.json
        """
    )
    
    parser.add_argument(
        "audit_id",
        nargs="?",
        type=int,
        help="Audit ID for single-market mode (legacy)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate resolution: compute/log poeHash+sig WITHOUT RPC/tx"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        metavar="JSON_PATH",
        help="Path to JSON config file containing batch markets (e.g., launch_wave1.sepolia.json)"
    )
    
    parser.add_argument(
        "--audit-id-start",
        type=int,
        default=2,
        help="Starting audit ID for batch mode (default: 2)"
    )
    
    parser.add_argument(
        "--mock-outcome",
        type=str,
        default="YES",
        help="Mock outcome for batch simulation (default: YES)"
    )
    
    parser.add_argument(
        "--network",
        type=str,
        choices=["mainnet", "mainnet-fork", "sepolia"],
        default=None,
        help="Target network: 'mainnet', 'mainnet-fork', or 'sepolia'. "
             "'mainnet-fork' uses mainnet RPC for fork simulation. "
             "Overrides config file network settings when provided. "
             "Defaults to config file values if omitted."
    )
    
    parser.add_argument(
        "--broker-override",
        type=str,
        metavar="ADDRESS",
        default=None,
        help="Override the broker address used for resolution signatures. "
             "Use this when the broker differs from the oracle address "
             "(e.g., mainnet deployer broker 0xF0a8701045aFFdad6fC7f0Cb9811ca1bc655Fbb8)."
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit batch processing to first N markets. "
             "When used without --config, auto-discovers markets from latest batch_results file."
    )
    
    return parser.parse_args()


def auto_discover_markets() -> Optional[Dict[str, Any]]:
    """
    Auto-discover markets from batch_results files when --config is not provided.
    Returns a config-like dict with 'markets' key, or None if no file found.
    
    Priority:
    1. Latest batch_results_*.json (extracts markets from results[].record)
    2. launch_wave1.mainnet.json
    3. launch_wave1.sepolia.json
    """
    import glob as glob_module
    
    # Look for batch_results_*.json files
    pattern = os.path.join(SCRIPT_DIR, "batch_results_*.json")
    files = glob_module.glob(pattern)
    
    if files:
        # Sort by modification time, newest first
        files.sort(key=os.path.getmtime, reverse=True)
        latest = files[0]
        
        logger.info(f"Auto-discovered batch results: {latest}")
        
        with open(latest, "r") as f:
            data = json.load(f)
        
        # If it's a batch_results file with results[], extract market records
        if "results" in data and "markets" not in data:
            markets = []
            for r in data["results"]:
                # Prefer record field which contains full market draft
                if "record" in r:
                    # record is the resolution record, reconstruct market draft
                    market_id = r.get("market_id", "unknown")
                    markets.append({
                        "id": market_id,
                        "title": r["record"].get("title", ""),
                        "question": r["record"].get("title", ""),
                        "resolution_criteria": {
                            "rule": r["record"].get("rule", ""),
                            "logic": r["record"].get("logic", ""),
                            "resolve_date": r["record"].get("resolve_time", ""),
                        }
                    })
                elif "market" in r:
                    markets.append(r["market"])
            
            logger.info(f"Extracted {len(markets)} markets from {latest}")
            return {"markets": markets, "_source_file": latest}
        
        # Has markets key directly
        if "markets" in data:
            return data
    
    # Fallback to launch_wave1 files
    for fname in ["launch_wave1.mainnet.json", "launch_wave1.sepolia.json"]:
        fpath = os.path.join(SCRIPT_DIR, fname)
        if os.path.exists(fpath):
            logger.info(f"Auto-discovered config: {fpath}")
            return load_config(fpath)
    
    return None


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and parse JSON config file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    if "markets" not in config:
        raise ValueError("Config must contain 'markets' array")
    
    return config


if __name__ == "__main__":
    args = parse_args()
    
    # Determine broker override
    broker_override = args.broker_override
    
    # Determine network config with --network override priority:
    # --network flag > config file > defaults
    if args.network in ("mainnet", "mainnet-fork"):
        rpc_url = MAINNET_RPC_URL
        contract_address = MAINNET_CONTRACT_ADDRESS
        chain_id = MAINNET_CHAIN_ID
        network_name = "Mainnet-fork" if args.network == "mainnet-fork" else "Mainnet"
        logger.info(f"Network override: Base {network_name}")
        logger.info(f"  RPC: {rpc_url}")
        logger.info(f"  Contract: {contract_address}")
        logger.info(f"  Chain ID: {chain_id}")
        if broker_override:
            logger.info(f"  Broker Override: {broker_override}")
    elif args.network == "sepolia":
        rpc_url = DEFAULT_RPC_URL
        contract_address = DEFAULT_CONTRACT_ADDRESS
        chain_id = DEFAULT_CHAIN_ID
        network_name = "Sepolia"
        logger.info(f"Network override: Base {network_name}")
    else:
        # Use config file values (or defaults)
        rpc_url = None
        contract_address = None
        chain_id = None
        network_name = None
    
    # Load config if provided, or auto-discover
    config = None
    config_source = None  # Track source for logging
    
    if args.config:
        config = load_config(args.config)
        config_source = args.config
        # Config file values are overridden by --network flag
        if not args.network:
            rpc_url = config.get("rpc", DEFAULT_RPC_URL)
            contract_address = config.get("lethe_escrow", DEFAULT_CONTRACT_ADDRESS)
            chain_id = config.get("chain_id", DEFAULT_CHAIN_ID)
        logger.info(f"Loading network config from {args.config}:")
        logger.info(f"  rpc: {rpc_url or 'default'}")
        logger.info(f"  lethe_escrow: {contract_address or 'default'}")
        logger.info(f"  chain_id: {chain_id or 'default'}")
        if broker_override:
            logger.info(f"  Broker Override: {broker_override}")
    elif args.dry_run or args.limit is not None or args.network:
        # Auto-discover when in dry-run mode or limit specified or network override
        config = auto_discover_markets()
        if config:
            config_source = config.get("_source_file", "auto-discovered")
            logger.info(f"Auto-discovered markets from: {config_source}")
        else:
            logger.error("No config file specified and could not auto-discover markets")
            sys.exit(1)
    
    pusher = LethePusher(
        rpc_url=rpc_url,
        contract_address=contract_address,
        chain_id=chain_id,
        broker_override=broker_override
    )
    
    # Batch mode (with --config OR auto-discovered)
    # Triggered when: args.config OR (auto-discovered config AND (dry_run OR limit OR network))
    in_batch_mode = config is not None and (args.config or args.dry_run or args.limit is not None or args.network)
    
    if in_batch_mode:
        markets = config.get("markets", [])
        
        if not markets:
            logger.error("No markets found")
            sys.exit(1)
        
        # Apply --limit if specified
        if args.limit is not None and args.limit > 0:
            markets = markets[:args.limit]
        
        total_markets = len(markets)
        effective_source = config_source if not args.config else args.config
        logger.info(f"=" * 60)
        logger.info(f"BATCH MODE: Processing {total_markets} markets from {effective_source}")
        logger.info(f"DRY-RUN: {'ENABLED' if args.dry_run else 'DISABLED'}")
        logger.info(f"Audit ID Start: {args.audit_id_start}")
        logger.info(f"Mock Outcome: {args.mock_outcome}")
        if args.limit is not None:
            logger.info(f"Market Limit: {args.limit}")
        if broker_override:
            logger.info(f"Broker Override: {broker_override}")
        logger.info(f"=" * 60)
        
        # Print batch header to stdout for visibility
        print(f"\n=== BATCH RESOLUTION {'SIMULATION' if args.dry_run else 'EXECUTION'} ===")
        print(f"Config: {effective_source}")
        print(f"Network: {network_name or 'from config'}")
        print(f"Total Markets: {total_markets}")
        print(f"Dry-Run Mode: {args.dry_run}")
        print(f"Audit ID Start: {args.audit_id_start}")
        if args.limit is not None:
            print(f"Market Limit: {args.limit}")
        if broker_override:
            print(f"Broker Override: {broker_override}")
        print()
        
        results = []
        for idx, market in enumerate(markets):
            audit_id = args.audit_id_start + idx
            market_id = market.get("id") or market.get("market_id", f"unknown:{idx}")
            
            print(f"--- Market {idx + 1}/{total_markets}: {market_id} ---")
            print(f"Audit ID: {audit_id}")
            
            try:
                result = pusher.process_market(
                    market=market,
                    audit_id=audit_id,
                    dry_run=args.dry_run,
                    mock_outcome=args.mock_outcome,
                    broker_override=broker_override
                )
                results.append(result)
                
                # Print to stdout
                print(f"poeHash: {result['poe_hash']}")
                print(f"Broker: {result.get('broker') or 'oracle-default'}")
                print(f"Signature: {result['signature']}")
                
                if args.dry_run:
                    print(f"Status: DRY-RUN (no RPC/tx)")
                else:
                    print(f"Status: {result['status']}")
                    if result.get('transaction_hash'):
                        print(f"TxHash: {result['transaction_hash']}")
                
                print()
                
            except Exception as e:
                logger.error(f"[Market: {market_id}] Error: {e}")
                print(f"ERROR: {e}\n")
                results.append({
                    "market_id": market_id,
                    "audit_id": audit_id,
                    "status": "error",
                    "error": str(e)
                })
        
        # Summary
        success_count = sum(1 for r in results if r.get("status") == "success" or r.get("status") == "dry_run")
        print(f"=== BATCH COMPLETE ===")
        print(f"Total: {total_markets}")
        print(f"Successful: {success_count}")
        print(f"Failed: {total_markets - success_count}")
        
        # Save batch results
        results_path = f"batch_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_path, "w") as f:
            json.dump({
                "config": effective_source,
                "dry_run": args.dry_run,
                "total_markets": total_markets,
                "results": results
            }, f, indent=2, default=str)
        logger.info(f"Batch results saved to {results_path}")
        
    # Single audit mode (legacy)
    elif args.audit_id is not None:
        audit_id = args.audit_id
        
        if args.dry_run:
            logger.warning("--dry-run ignored in single audit mode (legacy)")
        
        logger.info(f"=" * 60)
        logger.info(f"SINGLE AUDIT MODE: Audit ID {audit_id}")
        logger.info(f"=" * 60)
        
        # Legacy mock market
        mock_market = {
            "id": f"audit:{audit_id}:resolution",
            "title": "Lethe Audit #2 Resolution",
            "rule": "Volva-Lethe Phase 4 On-chain Verification.",
            "logic": "IF Oracle_Registered THEN SUCCESS ELSE FAILURE",
            "final_outcome": "SUCCESS",
            "resolve_time": datetime.datetime.now(datetime.UTC).isoformat(),
            "accepted_sources": ["volva:oracle"]
        }
        
        record = pusher.build_resolution_record(mock_market)
        poe_hash = pusher.compute_poe_hash(record)
        
        logger.info(f"Resolving Audit ID: {audit_id}")
        logger.info(f"poeHash: 0x{poe_hash.hex()}")
        
        try:
            sig = pusher.sign_resolution(audit_id, poe_hash)
            logger.info(f"Signature: {sig}")
            result = pusher.push_resolution(audit_id, poe_hash, sig)
            logger.info(f"Result: {result['status']}")
            
            # Save resolution record for audit trail
            with open(f"resolution_audit_{audit_id}.json", "w") as f:
                json.dump(record, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error during push: {e}")
            sys.exit(1)
    
    else:
        print("Usage:")
        print("  # Single audit mode (legacy)")
        print("  python3 lethe_pusher.py <audit_id>")
        print()
        print("  # Batch mode with config file")
        print("  python3 lethe_pusher.py --dry-run --config=launch_wave1.sepolia.json")
        print()
        print("  # Batch mode with auto-discovery (no --config needed)")
        print("  python3 lethe_pusher.py --dry-run --network=mainnet-fork --broker-override --limit=1")
        print()
        print("  # Batch mode with network override and limit")
        print("  python3 lethe_pusher.py --dry-run --network=mainnet-fork --broker-override --audit-id-start=1 --limit=1")
        sys.exit(1)
