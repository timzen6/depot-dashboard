# GitHub Copilot Instructions for Quality Core

> **General conventions** (Polars style, config, logging, file I/O, best practices): see [copilot-instructions.general.md](copilot-instructions.general.md).
> **Project overview, tech stack, architecture and high-level structure:** see [README.md](../README.md) — do not duplicate that content here.

## Project Context
- **Goal:** Local High-Conviction Portfolio Dashboard (details in README.md).

## Critical Guidelines

### Streamlit Architecture (MVC-Pattern)
- **Pages (`src/app/pages`)**: Minimal entry points. NO calculation logic.
- **Logic (`src/app/logic`)**: Data fetching, caching, and processing.
- **Views (`src/app/views`)**: Pure UI rendering (Plotly, Streamlit widgets).
- **Organization Rule**:
    - Simple Feature -> Single File (e.g., `overview.py`).
    - Complex Feature -> Subdirectory with `__init__.py` (e.g., `stock_detail/`).
