# Streamlit Application Structure (WI-08)

## Architecture Overview

The application follows a strict **MVC (Model-View-Controller)** pattern:

```
src/app/
├── main.py                          # Entry point (Welcome page)
├── logic/                           # Business Logic Layer (NO st.* calls)
│   ├── data_loader.py              # GlobalDataLoader with caching
│   ├── overview.py                  # Portfolio performance calculations
│   └── stock_detail/               # Complex feature (subdirectory)
│       ├── __init__.py
│       └── loader.py                # Ticker-specific data loading
├── views/                           # UI Rendering Layer (NO calculations)
│   ├── common.py                    # Shared components (KPI cards)
│   ├── overview.py                  # Portfolio charts & tables
│   └── stock_detail/               # Complex feature (subdirectory)
│       ├── __init__.py
│       └── charts.py                # Stock-specific charts
└── pages/                           # Page Layer (Wiring only)
    ├── 01_overview.py              # Portfolio overview page
    └── 02_stock_detail.py          # Stock detail page
```

## Layer Responsibilities

### Logic Layer (`src/app/logic/`)
- **Pure Python/Polars** - NO Streamlit calls
- Data loading, filtering, calculations
- Uses existing engines: `MetricsEngine`, `PortfolioEngine`
- Returns DataFrames or dictionaries

**Key Components:**
- `GlobalDataLoader`: Cached data loading with metric enrichment
- `overview.py`: Portfolio performance and KPIs
- `stock_detail/loader.py`: Ticker-specific data extraction

### View Layer (`src/app/views/`)
- **Pure Rendering** - NO data processing
- Only `st.*` and `plotly` calls
- Receives prepared data from logic layer
- Displays charts, tables, metrics

**Key Components:**
- `common.py`: Reusable UI components (KPI cards, empty states)
- `overview.py`: Portfolio charts and position tables
- `stock_detail/charts.py`: Price, valuation, and fundamental charts

### Page Layer (`src/app/pages/`)
- **Wiring Only** - Connects Logic to Views
- Handles user inputs (sidebar selectors)
- Minimal code - just orchestration
- Error handling and page layout

## Running the Application

```bash
# From project root
streamlit run src/app/main.py

# Alternative: Use make command if configured
make app
```

## Data Flow

```
User Action
    ↓
Page (01_overview.py)
    ↓
Logic (get_portfolio_performance)
    ↓
Engine (PortfolioEngine)
    ↓
← Returns DataFrame
    ↓
View (render_portfolio_chart)
    ↓
Streamlit UI
```

## Key Features

### 01 - Portfolio Overview
- Portfolio selector (sidebar)
- KPI cards (current value, returns, latest date)
- Portfolio value chart (line chart with fill)
- Portfolio composition (pie chart)
- Current positions table

### 02 - Stock Detail
- Ticker selector (sidebar)
- Date range filter
- **Three tabs:**
  - **Price & Volume**: Candlestick chart with volume bars
  - **Valuation**: FCF Yield & Dividend Yield over time
  - **Quality Metrics**: ROCE, Free Cash Flow charts

## Caching Strategy

- **`@st.cache_data(ttl=3600)`**: Data loads cached for 1 hour
- Automatically refreshes after expiry
- Spinner shown during initial load
- Separated from class methods for clean decorator usage

## Polars Best Practices

All data transformations use **method chaining**:

```python
df_result = (
    df_source
    .filter(pl.col("ticker") == selected_ticker)
    .select(["date", "close", "volume"])
    .sort("date")
)
```

## Type Hints & Documentation

- **Type hints** on all function signatures
- **Brief docstrings** explaining "why" and "what", not "how"
- No redundant type documentation (already in hints)

## Error Handling

- Try/except blocks in page layer
- Graceful degradation with `st.warning()` or `st.info()`
- Logging with `loguru.logger` for debugging
- Empty state rendering for missing data
