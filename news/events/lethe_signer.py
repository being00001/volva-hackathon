#!/usr/bin/env python3
"""
Volva Lethe Signer - Phase 2 Step 3
Generates resolution signatures for LetheEscrow.resolve.
Signs (auditId, poeHash) using the Volva Oracle's private key.

Usage:
    PYTHONUNBUFFERED=1 VOLVA_ORACLE_KEY=0x... python3 lethe_signer.py
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional, List, Dict

from eth_account import Account
from eth_account.messages import encode_defunct
from eth_abi import encode
from eth_utils import keccak
from surrealdb import AsyncSurreal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)-8s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("volva.signer")

# DB Config
SURREALDB_HOST = "http://127.0.0.1:8001"
SURREALDB_USER = "root"
SURREALDB_PASS = "root"
SURREALDB_NS = "volva"
SURREALDB_DB = "causal_graph"

class LetheSigner:
    def __init__(self):
        self.db: Optional[AsyncSurreal] = None
        # WHY: fallback to Volva's identity key instead of random — random key can't sign valid TXs
        self.oracle_key = os.environ.get(
            "VOLVA_ORACLE_KEY",
            "70768da9da8a188c5ad302bc04fb57e97e391aa64df060b67030854db9d7ce38",
        )
        
        self.account = Account.from_key(self.oracle_key)
        logger.info(f"Using Oracle Address: {self.account.address}")

    async def connect(self):
        self.db = AsyncSurreal(SURREALDB_HOST)
        await self.db.signin({"username": SURREALDB_USER, "password": SURREALDB_PASS})
        await self.db.use(namespace=SURREALDB_NS, database=SURREALDB_DB)
        logger.info("Connected to SurrealDB")

    async def close(self):
        if self.db:
            try:
                await self.db.close()
            except Exception:
                pass

    async def get_verified_predictions(self) -> List[Dict]:
        """Fetch verified predictions that have an external_audit_id but no signature."""
        query = "SELECT * FROM predicts WHERE status = 'verified' AND external_audit_id != NONE AND resolution_signature == NONE;"
        result = await self.db.query(query)
        
        preds = []
        if isinstance(result, list):
            if result and isinstance(result[0], dict) and "result" in result[0]:
                preds = result[0]["result"]
            else:
                preds = result
        return preds

    def generate_signature(self, audit_id: int, evidence_content: str) -> str:
        """
        Signs (audit_id, poeHash) using EIP-191.
        poeHash = keccak256(evidence_content)
        messageHash = keccak256(abi.encodePacked(audit_id, poeHash))
        """
        poe_hash = keccak(text=evidence_content)
        
        # messageHash = keccak256(abi.encodePacked(_auditId, _poeHash))
        encoded_message = encode(['uint256', 'bytes32'], [audit_id, poe_hash])
        message_hash = keccak(encoded_message)
        
        # Use sign_message for EIP-191 or signHash if available
        # To sign a raw hash without the Ethereum Signed Message prefix, use Account.signHash
        # But Lethe contract likely uses ecrecover which expects the prefix.
        # If the contract uses raw keccak256(abi.encodePacked(audit_id, poe_hash)), 
        # then we need to sign the hash of that.
        
        # In eth_account, sign_message takes a SignableMessage. 
        # encode_defunct(hexstr=message_hash.hex()) adds the prefix.
        
        # For testing, we'll try the common sign_message approach
        msg = encode_defunct(hexstr=message_hash.hex())
        signed_message = Account.sign_message(msg, self.oracle_key)
        return signed_message.signature.hex()

    async def run(self):
        await self.connect()
        try:
            preds = await self.get_verified_predictions()
            if not preds:
                logger.info("No verified predictions found needing signatures.")
                return

            for pred in preds:
                pred_id = pred['id']
                audit_id = int(pred['external_audit_id'])
                
                # Fetch evidence event content
                evidence_ref = pred['evidence_event']
                ev_result = await self.db.query(f"SELECT content FROM {evidence_ref};")
                
                ev_data = []
                if isinstance(ev_result, list):
                    if ev_result and isinstance(ev_result[0], dict) and "result" in ev_result[0]:
                        ev_data = ev_result[0]["result"]
                    else:
                        ev_data = ev_result
                
                if not ev_data:
                    logger.warning(f"No evidence content found for {pred_id}")
                    continue
                    
                evidence_content = ev_data[0]['content']

                logger.info(f"Signing resolution for prediction {pred_id} (Audit {audit_id})")
                signature = self.generate_signature(audit_id, evidence_content)
                
                # Update DB with signature
                await self.db.query(f"UPDATE {pred_id} SET resolution_signature = '{signature}';")
                logger.info(f"Signature generated and stored: {signature}")

        finally:
            await self.close()

if __name__ == "__main__":
    asyncio.run(LetheSigner().run())
