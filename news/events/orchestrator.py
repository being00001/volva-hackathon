#!/usr/bin/env python3
"""
Volva Pipeline Orchestrator - Phase 0-5
Automates the full Völva data ingestion pipeline by executing scripts in sequence.

Usage:
    python3 orchestrator.py [--settlement] [--dry-run]

Options:
    --settlement    Run settlement loop (judgement.py + lethe_pusher.py)
    --dry-run       Print commands without executing

Pipeline Stages:
    1. scraper.py          - RSS feed ingestion
    2. causal_linker.py    - Causal link discovery (24h window)
    3. cross_linker.py     - Cross-link discovery (Phase 5, multi-window)
    4. chain_discoverer.py - Transitive chain discovery
    5. predictor.py        - Prediction generation
    6. market_transformer.py - Market transformation
    7. judgement.py       - Prediction verification (settlement)
    8. lethe_pusher.py     - On-chain settlement (settlement)
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_FILE = SCRIPT_DIR / "orchestrator.log"

# Pipeline stage definitions
# Stages are executed in order. Each tuple is (display_name, script_filename)
REQUIRED_STAGES = [
    ("scraper", "scraper.py"),
    ("causal_linker", "causal_linker.py"),
    ("cross_linker", "cross_linker.py"),  # Phase 5: Automated Scaling
    ("chain_discoverer", "chain_discoverer.py"),
    ("predictor", "predictor.py"),
    ("market_transformer", "market_transformer.py"),
]

SETTLEMENT_STAGES = [
    ("judgement", "judgement.py"),
    ("lethe_pusher", "lethe_pusher.py"),
]

FEEDBACK_STAGES = [
    ("feedback_loop", "feedback_loop.py"),
]

# Stage9: iter3 loop for autonomous virtual ledger self-improve
# Runs iter2_resolve_positions.py (resolves Polymarket positions) then feedback_loop.py
ITER3_STAGES = [
    ("iter2_resolve", "qn-virtual-polymarket-ledger/iter2_resolve_positions.py"),
    ("feedback_loop", "feedback_loop.py"),
]


def setup_logging(verbose: bool = True) -> logging.Logger:
    """Configure dual-output logging to console and file."""
    logger = logging.getLogger("volva.orchestrator")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(LOG_FILE, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger


# Stage-specific timeouts (seconds)
# cross_linker.py is the most complex (Gemini embeddings + NER + multi-window)
STAGE_TIMEOUTS = {
    "scraper": 1800,          # 30 min - fast
    "causal_linker": 1800,    # 30 min - moderate
    "cross_linker": 14400,     # 4 hours - Gemini embeddings + NER + multi-window (increased from 7200)
    "chain_discoverer": 1800, # 30 min - moderate
    "predictor": 1800,        # 30 min - moderate
    "market_transformer": 1800,  # 30 min - moderate
    "judgement": 3600,       # 1 hour - settlement can be slow
    "lethe_pusher": 3600,     # 1 hour - settlement can be slow
    "feedback_loop": 1800,    # 30 min - moderate
    "iter2_resolve": 1800,    # 30 min - moderate
}


def run_script(script_name: str, script_path: Path, logger: logging.Logger, dry_run: bool = False) -> tuple[bool, float]:
    """
    Execute a single pipeline script via subprocess.
    
    Returns:
        Tuple of (success: bool, duration_seconds: float)
    """
    stage_name = script_name.upper()
    cmd = [sys.executable, str(script_path)]
    timeout = STAGE_TIMEOUTS.get(script_name, 3600)  # Default 1 hour
    
    logger.info(f"[{stage_name}] Starting {script_name}...")
    start_time = time.time()
    
    if dry_run:
        logger.info(f"[{stage_name}] DRY RUN: {' '.join(cmd)}")
        return True, 0.0
    
    try:
        # Pass DRY_RUN=true environment variable if in dry-run mode
        env = os.environ.copy()
        if dry_run:
            env["DRY_RUN"] = "true"
        
        result = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout,
        )
        
        duration = time.time() - start_time
        
        # Log script output
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    logger.debug(f"[{stage_name}] {line}")
        
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                if line.strip():
                    logger.warning(f"[{stage_name}] {line}")
        
        if result.returncode == 0:
            logger.info(f"[{stage_name}] Completed successfully in {duration:.2f}s")
            return True, duration
        else:
            logger.error(f"[{stage_name}] Failed with exit code {result.returncode} in {duration:.2f}s")
            return False, duration
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        logger.error(f"[{stage_name}] Timed out after {duration:.2f}s")
        return False, duration
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{stage_name}] Exception: {e} (after {duration:.2f}s)")
        return False, duration


def print_summary(
    logger: logging.Logger,
    stage_results: list[tuple[str, bool, float]],
    total_duration: float,
    settlement_results: Optional[list[tuple[str, bool, float]]] = None,
    feedback_results: Optional[list[tuple[str, bool, float]]] = None,
    iter3_results: Optional[list[tuple[str, bool, float]]] = None
):
    """Print final pipeline summary."""
    separator = "=" * 60
    logger.info(separator)
    logger.info("PIPELINE SUMMARY")
    logger.info(separator)
    
    # Required stages
    logger.info("Required Stages:")
    all_passed = True
    for stage_name, success, duration in stage_results:
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"  {status}  {stage_name:<20} {duration:>8.2f}s")
        if not success:
            all_passed = False
    
    # Settlement stages
    if settlement_results is not None:
        logger.info("Settlement Stages:")
        for stage_name, success, duration in settlement_results:
            status = "✓ PASS" if success else "✗ FAIL"
            logger.info(f"  {status}  {stage_name:<20} {duration:>8.2f}s")
    
    # Feedback stages
    if feedback_results is not None:
        logger.info("Feedback Stages:")
        for stage_name, success, duration in feedback_results:
            status = "✓ PASS" if success else "✗ FAIL"
            logger.info(f"  {status}  {stage_name:<20} {duration:>8.2f}s")
    
    # iter3 stages (Stage9)
    if iter3_results is not None:
        logger.info("Stage9 - iter3 Loop:")
        for stage_name, success, duration in iter3_results:
            status = "✓ PASS" if success else "✗ FAIL"
            logger.info(f"  {status}  {stage_name:<20} {duration:>8.2f}s")
    
    # Total duration
    logger.info(separator)
    logger.info(f"Total Duration: {total_duration:.2f}s")
    logger.info(f"Overall Status: {'✓ SUCCESS' if all_passed else '✗ FAILED'}")
    logger.info(separator)
    
    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Volva Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 orchestrator.py                # Run main pipeline
  python3 orchestrator.py --settlement    # Run with settlement loop
  python3 orchestrator.py --feedback      # Run feedback loop
  python3 orchestrator.py --settlement --feedback  # Run with both
  python3 orchestrator.py --iter3         # Run Stage9 iter3 loop (autonomous self-improve)
  python3 orchestrator.py --dry-run        # Show commands without running
        """
    )
    parser.add_argument(
        "--settlement",
        action="store_true",
        help="Run settlement loop (judgement.py + lethe_pusher.py)"
    )
    parser.add_argument(
        "--feedback",
        action="store_true",
        help="Run feedback loop to update causal link confidences"
    )
    parser.add_argument(
        "--iter3",
        action="store_true",
        help="Run Stage9 iter3 loop (iter2_resolve_positions.py + feedback_loop.py)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    # Banner
    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("=" * 60)
    logger.info("VÖLVA PIPELINE ORCHESTRATOR")
    logger.info(f"Run started at: {run_time}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Settlement loop: {'ENABLED' if args.settlement else 'DISABLED'}")
    logger.info(f"Feedback loop:   {'ENABLED' if args.feedback else 'DISABLED'}")
    logger.info(f"Stage9 iter3:     {'ENABLED' if args.iter3 else 'DISABLED'}")
    logger.info("=" * 60)
    
    overall_start = time.time()
    stage_results: list[tuple[str, bool, float]] = []
    settlement_results: list[tuple[str, bool, float]] = []
    feedback_results: list[tuple[str, bool, float]] = []
    iter3_results: list[tuple[str, bool, float]] = []
    
    # Run required stages
    for stage_name, script_name in REQUIRED_STAGES:
        script_path = SCRIPT_DIR / script_name
        
        if not script_path.exists():
            logger.error(f"[{stage_name.upper()}] Script not found: {script_path}")
            print_summary(logger, stage_results, time.time() - overall_start)
            sys.exit(1)
        
        success, duration = run_script(stage_name, script_path, logger, args.dry_run)
        stage_results.append((stage_name, success, duration))
        
        # Abort on failure
        if not success and not args.dry_run:
            logger.error(f"Pipeline aborted at stage: {stage_name}")
            print_summary(logger, stage_results, time.time() - overall_start)
            sys.exit(1)
    
    # Run settlement loop if enabled
    if args.settlement:
        logger.info("-" * 60)
        logger.info("SETTLEMENT LOOP STARTING")
        logger.info("-" * 60)
        
        for stage_name, script_name in SETTLEMENT_STAGES:
            script_path = SCRIPT_DIR / script_name
            
            if not script_path.exists():
                logger.warning(f"[{stage_name.upper()}] Script not found: {script_path} (skipping)")
                settlement_results.append((stage_name, True, 0.0))
                continue
            
            # Settlement stages are best-effort (don't abort pipeline)
            success, duration = run_script(stage_name, script_path, logger, args.dry_run)
            settlement_results.append((stage_name, success, duration))
            
            if not success:
                logger.warning(f"[{stage_name.upper()}] Settlement stage failed (continuing)")
    
    # Run feedback loop if enabled
    if args.feedback:
        logger.info("-" * 60)
        logger.info("FEEDBACK LOOP STARTING")
        logger.info("-" * 60)
        
        for stage_name, script_name in FEEDBACK_STAGES:
            script_path = SCRIPT_DIR / script_name
            
            if not script_path.exists():
                logger.warning(f"[{stage_name.upper()}] Script not found: {script_path} (skipping)")
                feedback_results.append((stage_name, True, 0.0))
                continue
            
            # Feedback stages are best-effort (don't abort pipeline)
            success, duration = run_script(stage_name, script_path, logger, args.dry_run)
            feedback_results.append((stage_name, success, duration))
            
            if not success:
                logger.warning(f"[{stage_name.upper()}] Feedback stage failed (continuing)")
    
    # Run Stage9 iter3 loop if enabled
    if args.iter3:
        logger.info("-" * 60)
        logger.info("STAGE9 ITER3 LOOP STARTING")
        logger.info("-" * 60)
        
        for stage_name, script_name in ITER3_STAGES:
            script_path = SCRIPT_DIR / script_name
            
            if not script_path.exists():
                logger.warning(f"[{stage_name.upper()}] Script not found: {script_path} (skipping)")
                iter3_results.append((stage_name, True, 0.0))
                continue
            
            # iter3 stages are best-effort (don't abort pipeline)
            success, duration = run_script(stage_name, script_path, logger, args.dry_run)
            iter3_results.append((stage_name, success, duration))
            
            if not success:
                logger.warning(f"[{stage_name.upper()}] iter3 stage failed (continuing)")
    
    # Calculate total duration
    total_duration = time.time() - overall_start
    
    # Print final summary
    success = print_summary(logger, stage_results, total_duration, 
                            settlement_results if args.settlement else None,
                            feedback_results if args.feedback else None,
                            iter3_results if args.iter3 else None)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
