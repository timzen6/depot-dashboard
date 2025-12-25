BASE_BRANCH ?= origin/main
EXCLUDES := ':!uv.lock' ':!*.pyc' ':!data/*' ':!llm_inputs/*'

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

meta:
	uv run qc meta

fundamentals:
	uv run qc fundamentals

etl:
	uv run qc etl

etl-full:
	uv run qc etl --full

snapshot:
	uv run qc snapshot

test:
	uv run --extra dev pytest

app:
	uv run streamlit run src/app/00_Startpage.py

project-export:
	@gh project item-list 1 --owner timzen6 --format json > llm_inputs/project_items.json
	@cat llm_inputs/project_items.json | pbcopy
	@echo "✅ Project items exported to llm_inputs/project_items.json and copied to clipboard"

pr-diff: pc
	@mkdir -p llm_inputs
	@echo "🔍 Fetching latest $(BASE_BRANCH)..."
	@git fetch origin main
	@echo "🔍 Calculating diff..."
	@{ \
		START_POINT=$$(git merge-base $(BASE_BRANCH) HEAD); \
		echo "# PR Context Report"; \
		echo "Generated on: $$(date)"; \
		echo ""; \
		echo "## 1. Commit History (unique to Branch)"; \
		git log --format="- %s" $$START_POINT..HEAD; \
		echo ""; \
		echo "## 2. Code Change Stats (unique to Branch)"; \
		git diff --stat $$START_POINT..HEAD -- . $(EXCLUDES); \
		echo ""; \
		echo "## 3. Code Diff (unique to Branch)"; \
		git diff $$START_POINT..HEAD -- . $(EXCLUDES); \
	} > llm_inputs/pr_context_report.txt
	@cat llm_inputs/pr_context_report.txt | pbcopy
	@echo "✅ PR context saved to llm_inputs/pr_context_report.txt and copied to clipboard."
