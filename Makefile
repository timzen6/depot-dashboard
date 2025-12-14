.PHONY: pc etl snapshot restore test help

help:
	@echo "Available commands:"
	@echo "  make pc         - Run pre-commit checks"
	@echo "  make etl        - Run ETL pipeline"
	@echo "  make snapshot   - Create data snapshots"
	@echo "  make test       - Run tests"

pc:
	uv run --extra dev pre-commit run --all-files

etl:
	uv run qc etl

snapshot:
	uv run qc snapshot

test:
	uv run --extra dev pytest
