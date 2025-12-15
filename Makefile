.PHONY: pc etl snapshot restore test app help

help:
	@echo "Available commands:"
	@echo "  make pc         - Run pre-commit checks"
	@echo "  make etl        - Run ETL pipeline"
	@echo "  make snapshot   - Create data snapshots"
	@echo "  make test       - Run tests"
	@echo "  make app        - Launch Streamlit dashboard"

pc:
	uv run --extra dev pre-commit run --all-files

etl:
	uv run qc etl

snapshot:
	uv run qc snapshot

test:
	uv run --extra dev pytest

app:
	uv run streamlit run src/app/main.py
