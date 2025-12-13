# GitHub Copilot Instructions for Quality Core

## Project Context
- **Goal:** Local High-Conviction Portfolio Dashboard.
- **Stack:** Python 3.11+, Polars, Streamlit, Pydantic, yfinance.

## Critical Guidelines

### 1. Polars & Data
- **Mode:** Use **Eager execution** (`DataFrame`) by default for readability. Use `LazyFrame` only for `scan_parquet` or heavy aggregations.
- **Style:** Strictly use **method chaining**.
- **Pandas:** Only allowed at the I/O boundary. Convert to Polars immediately.

### 2. Streamlit Architecture (MVC-Pattern)
- **Pages (`src/app/pages`)**: Minimal entry points. NO calculation logic.
- **Logic (`src/app/logic`)**: Data fetching, caching, and processing.
- **Views (`src/app/views`)**: Pure UI rendering (Plotly, Streamlit widgets).
- **Organization Rule**:
    - Simple Feature -> Single File (e.g., `overview.py`).
    - Complex Feature -> Subdirectory with `__init__.py` (e.g., `stock_detail/`).

### 3. File I/O
- **Atomic Writes:** Always write to `.tmp` and rename to target path.
- **Paths:** Use `pathlib.Path`.

## Project File Structure (Strict Reference)

```text
quality-core/
├── data/
│   ├── input/                  # User data (Depot exports .csv/.xlsx, Watchlists)
│   ├── prices/                 # Parquet files for daily OHLCV
│   └── fundamentals/           # Parquet files for financial metrics
├── docs/                       # Architecture decisions (ADR) & Structure
├── src/
│   ├── app/                    # Streamlit Application
│   │   ├── common/             # Shared Widgets (Layouts, Cards)
│   │   ├── pages/              # Entry Points (Wiring only)
│   │   │   ├── 01_overview.py
│   │   │   └── 02_stock_detail.py
│   │   ├── logic/              # Business Logic (Data & Calc)
│   │   │   ├── overview.py             # Simple logic file
│   │   │   └── stock_detail/           # Complex logic package
│   │   │       ├── __init__.py         # Exposes main logic class/func
│   │   │       ├── metrics.py          # Specific metric calc
│   │   │       └── helpers.py
│   │   ├── views/              # UI Rendering (No logic)
│   │   │   ├── overview.py             # Simple view file
│   │   │   └── stock_detail/           # Complex view package
│   │   │       ├── __init__.py         # Exposes render() function
│   │   │       ├── charts.py           # Charting functions
│   │   │       └── financial_tab.py    # Sub-component
│   │   └── main.py             # App Root
│   ├── core/                   # Domain Core (Shared)
│   │   ├── models.py           # Pydantic Domain Models
│   │   ├── storage.py          # Parquet I/O Engine (Atomic)
│   │   └── types.py            # Enums
│   └── etl/                    # Data Pipeline
│       ├── pipeline.py         # Orchestration
│       └── mapper.py           # yfinance -> Domain Model
├── pyproject.toml
└── uv.lock
```


### Best Practices
- Always use type hints
- Document functions with short docstrings, only say the why and explain complicated parts, we do not need to all input and return types documented
- Keep functions pure where possible, prefer functional style
- Follow method chaining pattern for data transformations
- strive for readability, rather than excessive checking and precautions
