#!/bin/bash
#===============================================================================
# Volva Kite AI Demo - E2E Orchestration Script
# 
# This script runs the full E2E demo for the Kite AI hackathon.
# It starts the mock payment server and runs the demo in sequence.
#
# Usage:
#   bash demo.sh              # Full E2E demo
#   bash demo.sh --dry-run    # Simulation only
#   bash demo.sh --mock-only  # Start mock server only
#===============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$SCRIPT_DIR"
MOCK_PID=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse arguments
DRY_RUN=false
MOCK_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --mock-only)
            MOCK_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [--dry-run] [--mock-only]"
            echo "  --dry-run    Run simulation only (no real transactions)"
            echo "  --mock-only  Start mock payment server only"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

#===============================================================================
# Step 0: Prerequisites Check
#===============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "   Volva Kite AI Hackathon Demo - E2E Orchestration"
echo "═══════════════════════════════════════════════════════════════"

log_info "Checking prerequisites..."

# Check Node.js
if ! command -v node &> /dev/null; then
    log_error "Node.js is required but not installed."
    exit 1
fi

NODE_VERSION=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
if [[ "$NODE_VERSION" -lt 18 ]]; then
    log_error "Node.js 18+ is required. Current version: v$NODE_VERSION"
    exit 1
fi

log_success "Node.js $(node --version) detected"

# Check npm
if ! command -v npm &> /dev/null; then
    log_error "npm is required but not installed."
    exit 1
fi

log_success "npm $(npm --version) detected"

#===============================================================================
# Step 1: Install Dependencies
#===============================================================================
echo ""
echo "───────────────────────────────────────────────────────────────"
echo "STEP 1: Installing Dependencies"
echo "───────────────────────────────────────────────────────────────"

cd "$DEMO_DIR"

if [[ ! -d "node_modules" ]]; then
    log_info "Installing npm packages..."
    npm install
    log_success "Dependencies installed"
else
    log_info "Dependencies already installed"
fi

#===============================================================================
# Step 2: Start Mock Payment Server
#===============================================================================
echo ""
echo "───────────────────────────────────────────────────────────────"
echo "STEP 2: Starting Mock Payment Server (kite.pay x402)"
echo "───────────────────────────────────────────────────────────────"

log_info "Starting mock server in background..."
node "$DEMO_DIR/kite_pay_mock.js" &
MOCK_PID=$!

# Wait for server to start
sleep 2

# Check if server is running
if ! kill -0 $MOCK_PID 2>/dev/null; then
    log_error "Mock server failed to start"
    exit 1
fi

log_success "Mock server running (PID: $MOCK_PID)"
log_info "Server URL: http://localhost:8080"

# Test mock server
log_info "Testing mock server health..."
HEALTH=$(curl -s http://localhost:8080/health 2>/dev/null || echo '{"error":"failed"}')
if echo "$HEALTH" | grep -q '"status":"ok"'; then
    log_success "Mock server health check passed"
else
    log_warn "Mock server health check failed, continuing anyway..."
fi

# If mock-only mode, stay running
if [[ "$MOCK_ONLY" == true ]]; then
    log_info "Mock server running. Press Ctrl+C to stop."
    wait $MOCK_PID
    exit 0
fi

#===============================================================================
# Step 3: Run Demo
#===============================================================================
echo ""
echo "───────────────────────────────────────────────────────────────"
echo "STEP 3: Running Volva Kite AI Demo"
echo "───────────────────────────────────────────────────────────────"

if [[ "$DRY_RUN" == true ]]; then
    log_info "Running in DRY-RUN mode (simulation only)"
    cd "$DEMO_DIR"
    node demo.js --dry-run
else
    log_warn "Running in LIVE mode"
    log_warn "This will send real transactions to Kite testnet"
    log_warn "Make sure you have faucet funds before proceeding!"
    echo ""
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Aborted by user"
        kill $MOCK_PID 2>/dev/null || true
        exit 0
    fi
    
    cd "$DEMO_DIR"
    node demo.js
fi

DEMO_EXIT_CODE=$?

#===============================================================================
# Step 4: Test Payment Flow
#===============================================================================
echo ""
echo "───────────────────────────────────────────────────────────────"
echo "STEP 4: Testing kite.pay() x402 Flow"
echo "───────────────────────────────────────────────────────────────"

log_info "Testing payment-required endpoint (should return 402)..."
RESPONSE=$(curl -s -w "\n%{http_code}" http://localhost:8080/api/v1/signals)
BODY=$(echo "$RESPONSE" | head -n -1)
STATUS=$(echo "$RESPONSE" | tail -n 1)

if [[ "$STATUS" == "402" ]]; then
    log_success "✓ Received HTTP 402 (Payment Required)"
    log_info "X-PAYMENT header present: $(echo "$BODY" | grep -q 'X-PAYMENT' && echo 'yes' || echo 'no')"
else
    log_warn "Expected 402, got $STATUS"
fi

log_info "Testing premium endpoint with auto-settlement..."
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"wallet":"0xTestWallet123","amount":"1.00","currency":"USDC"}' \
    http://localhost:8080/api/v1/signals/premium)

if echo "$RESPONSE" | grep -q '"success":true'; then
    log_success "✓ Premium signals received after settlement"
else
    log_warn "Premium endpoint response: $RESPONSE"
fi

#===============================================================================
# Step 5: Cleanup
#===============================================================================
echo ""
echo "───────────────────────────────────────────────────────────────"
echo "STEP 5: Cleanup"
echo "───────────────────────────────────────────────────────────────"

log_info "Stopping mock server (PID: $MOCK_PID)..."
kill $MOCK_PID 2>/dev/null || true
sleep 1

if kill -0 $MOCK_PID 2>/dev/null; then
    log_warn "Mock server still running, forcing kill..."
    kill -9 $MOCK_PID 2>/dev/null || true
fi

log_success "Mock server stopped"

#===============================================================================
# Summary
#===============================================================================
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "   E2E DEMO COMPLETE"
echo "═══════════════════════════════════════════════════════════════"

if [[ "$DRY_RUN" == true ]]; then
    log_info "Mode: DRY-RUN (simulation only)"
else
    log_info "Mode: LIVE (transactions submitted)"
fi

echo ""
log_info "📹 For submission:"
echo "   1. Run: npm run demo:dry-run  (or bash demo.sh --dry-run)"
echo "   2. Record terminal output"
echo "   3. Visit explorer.kiteai.net to verify any submitted txs"
echo "   4. Use video_script.md for narration"
echo ""
log_success "Demo artifacts ready in: $DEMO_DIR"
echo ""

exit $DEMO_EXIT_CODE
