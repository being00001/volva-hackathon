#!/bin/bash
#
# Volva Arc Testnet Demo - Orchestration Script
#
# MIT License
#
# Usage:
#   ./demo.sh --dry-run    # Offline simulation (default)
#   ./demo.sh --live       # Real testnet transactions
#   ./demo.sh --help       # Show help
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default mode
MODE="dry-run"
TX_COUNT="50"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            MODE="dry-run"
            shift
            ;;
        --live)
            MODE="live"
            shift
            ;;
        --count)
            TX_COUNT="$2"
            shift 2
            ;;
        --help|-h)
            echo "Volva Arc Testnet Demo"
            echo "======================"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --dry-run        Run simulation (default, works offline)"
            echo "  --live           Run on real Arc testnet (needs funded wallet)"
            echo "  --count N        Number of transactions (default: 50)"
            echo "  --help, -h       Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                     # Dry-run with 50 transactions"
            echo "  $0 --dry-run --count 100  # 100 simulated transactions"
            echo "  $0 --live              # 50 real testnet transactions"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check Node.js version
NODE_VERSION=$(node --version 2>/dev/null | cut -d'v' -f2 | cut -d'.' -f1)
if [[ -z "$NODE_VERSION" ]]; then
    echo "❌ Error: Node.js is not installed"
    exit 1
fi

if [[ "$NODE_VERSION" -lt 22 ]]; then
    echo "⚠️  Warning: Node.js 22+ recommended, found v${NODE_VERSION}"
fi

# Ensure npm dependencies are installed
if [[ ! -d "node_modules" ]]; then
    echo "📦 Installing npm dependencies..."
    npm install
    if [[ $? -ne 0 ]]; then
        echo "❌ Failed to install npm dependencies"
        exit 1
    fi
    echo "✅ Dependencies installed"
fi

# Log file
LOG_FILE="demo_output.log"

echo "═══════════════════════════════════════════════════════════════"
echo "   Volva Arc Testnet Demo"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📡 Mode: ${MODE}"
echo "📦 Transactions: ${TX_COUNT}"
echo "📄 Log file: ${LOG_FILE}"
echo ""

# Build command
CMD="node arc_demo.js --count ${TX_COUNT}"
if [[ "$MODE" == "live" ]]; then
    CMD="$CMD --live"
else
    CMD="$CMD --dry-run"
fi

# Run demo and capture output
echo "🚀 Starting demo..."
echo ""

# Run the demo, tee to both stdout and log file
eval "$CMD" 2>&1 | tee "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "═══════════════════════════════════════════════════════════════"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "   ✅ Demo completed successfully"
else
    echo "   ❌ Demo failed with exit code $EXIT_CODE"
fi
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "📄 Output saved to: ${LOG_FILE}"

exit $EXIT_CODE
