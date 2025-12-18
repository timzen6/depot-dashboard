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

pr-diff: pc
	@mkdir -p llm_inputs
	@echo "🔍 Fetching latest $(BASE_BRANCH)..."
	@git fetch origin main
	@echo "🔍 Calculating diff..."
	@{ \
		START_POINT=$$(git merge-base $(BASE_BRANCH) HEAD); \
		echo "# PR Context Report"; \
		echo "Generated on: $$(date)"; \
		echo "\n## 1. Commit History (unique to Branch)"; \
		git log --format="- %s" $$START_POINT..HEAD; \
		echo "\n## 2. Code Change Stats (unique to Branch)"; \
		git diff --stat $$START_POINT..HEAD -- . $(EXCLUDES); \
		echo "\n## 3. Code Diff (unique to Branch)"; \
		git diff $$START_POINT..HEAD -- . $(EXCLUDES); \
	} > llm_inputs/pr_context_report.txt
	@cat llm_inputs/pr_context_report.txt | pbcopy
	@echo "✅ PR context saved to llm_inputs/pr_context_report.txt and copied to clipboard."
