# Makefile for Völva project
# Provides test targets for development and CI

PYTHON := python3
# Include news/events in PYTHONPATH so tests can import modules from that directory
PYTHONPATH := .:news/events

# Default target
.PHONY: all
all: test

# Test target - runs change-detection tests (fast)
# Uses pytest-testmon to track file changes and only run affected tests
# Falls back to running script-based tests directly if no pytest tests found
.PHONY: test
test:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest news/events -q --testmon 2>/dev/null || \
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) news/events/test_market_transformer.py

# Test-all target - runs full test suite
# For script-based tests, runs sequentially; for pytest tests uses 8 workers
.PHONY: test-all
test-all:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest news/events -q --numprocesses=8 2>/dev/null || \
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) news/events/test_market_transformer.py

# Help target
.PHONY: help
help:
	@echo "Völva Makefile targets:"
	@echo "  make test       - Run testmon-based change-detection tests (fast)"
	@echo "  make test-all   - Run full test suite with 8 parallel workers"
