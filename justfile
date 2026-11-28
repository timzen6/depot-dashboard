set dotenv-load := true
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

base_branch := env("BASE_BRANCH", "origin/main")

# Show available commands
default:
    @just --list

# Run pre-commit checks on all files
pc:
    uv run pre-commit run --all-files

# Run CI quality checks (lint, format, type-check)
check:
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy src

# Update asset metadata (e.g. just meta --full)
meta *flags:
    uv run qc meta {{flags}}

# Run fundamentals ETL pipeline
fundamentals:
    uv run qc fundamentals

# Run metadata + ETL pipeline (e.g. just etl --full)
etl *flags:
    uv run qc meta {{flags}}
    uv run qc etl {{flags}}

# Create compressed data snapshots
snapshot:
    uv run qc snapshot

# Run tests (e.g. just test -k foo)
test *args:
    uv run pytest {{args}}

# Launch the Streamlit dashboard
app:
    uv run streamlit run src/app/00_Startpage.py

# Bundle project docs into a Markdown payload and copy to clipboard
get-context:
    uv run qc dev context

# Export GitHub Project items to llm_inputs/project_items.json
project-export:
    uv run qc dev project-export

# Run pre-commit, then generate a PR diff report against the base branch
pr-diff branch=base_branch: pc
    uv run qc dev pr-diff --base {{branch}}

# Start Aider with Mistral for token-efficient pair programming (.env is loaded by just)
aider:
    aider --config .aider.conf.yaml
    # uvx --from aider-chat --with "scipy>=1.17.1" aider --config .aider.conf.yaml
