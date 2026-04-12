# Streamlit Application Strategy

Learnings and patterns from building a Streamlit-based financial dashboard. Not exhaustive — just what we know works, captured so we don't start from zero next time.

---

## Part 1: General Blueprint

### 1. The Core Problem with Streamlit

Streamlit re-runs scripts top-to-bottom on every interaction. Without structure, this leads to:
- Spaghetti code mixing data loading, computation, and UI in one file.
- Redundant recomputation killing responsiveness.
- Unmaintainable pages that grow to 500+ lines.

The entire strategy below exists to **tame the re-run model**.

---

### 2. MVC Layering — The Non-Negotiable Foundation

Split every feature across three layers:

```
src/app/
├── pages/     # Controller — wiring only
├── logic/     # Model — pure Python, no st.* calls
└── views/     # View — pure rendering, no computation
```

#### Pages (Controller)
- Max ~100 lines. If a page grows beyond that, logic or rendering leaked in.
- Responsibilities: `set_page_config`, sidebar widgets, call logic, pass results to views, error handling.
- Use `st.stop()` as early returns when data is missing or selections are invalid.

```python
# pages/01_overview.py — ideal shape
st.set_page_config(page_title="Overview", layout="wide")

data = load_data()
if not data:
    render_empty_state("No data available")
    st.stop()

selection = st.sidebar.selectbox("Pick one", options)
result = compute_something(data, selection)   # logic call
render_result_chart(result)                    # view call
```

#### Logic (Model)
- **Zero** Streamlit imports. Pure Python/Polars functions.
- Takes DataFrames or domain objects as input, returns DataFrames or dataclasses.
- Testable in isolation — no browser needed.

#### Views (Rendering)
- **Zero** data loading or heavy computation.
- Functions named `render_*()`, receiving prepared data.
- All Plotly/Streamlit widget calls live here.

**Why this matters:** Logic and views become independently testable. If you need to swap Plotly for Altair, only view files change. If data schema shifts, only logic files change.

---

### 3. The Complexity Scaler

Not every feature needs a package. Scale the file structure to match complexity:

| Complexity | Logic | View |
|---|---|---|
| **Simple** (single chart, one table) | `logic/feature.py` | `views/feature.py` |
| **Complex** (multiple tabs, charts, tables) | `logic/feature/` package | `views/feature/` package |

For packages, use `__init__.py` to expose a clean public API. Internal modules stay private.

---

### 4. Caching Strategy

Caching is the single most impactful performance lever in Streamlit.

#### Rules
1. **Cache at the data loading boundary**, not deep inside logic.
2. Use `@st.cache_data(ttl=<seconds>)` for DataFrames. Use `@st.cache_resource` for connections, engines, or objects that can't be serialized.
3. **Keep cached functions as module-level free functions**, not class methods — Streamlit's cache serializes arguments, and `self` doesn't serialize cleanly.

```python
# Pattern: class delegates to a cached free function
class DataLoader:
    def load(self) -> DataFrame:
        return _load_cached(self.path)

@st.cache_data(ttl=3600)
def _load_cached(path: Path) -> DataFrame:
    return pl.scan_parquet(path / "*.parquet").collect()
```

#### Anti-Patterns
- Caching a function that takes a mutable argument (DataFrame contents change → cache miss every time).
- Caching fine-grained computations — overhead of hashing often exceeds the compute saved.
- Forgetting TTL — stale data silently corrupts the UI.

#### Lazy vs. Eager Polars
Avoid premature optimization. Polars is fast enough in eager mode for dashboards of this scale. Use lazy execution (`LazyFrame`) only if profiling shows a real bottleneck — not as a default.

---

### 5. Data Container Pattern

Avoid passing 5+ DataFrames through every function. Bundle related data into a typed container:

```python
@dataclass
class DashboardData:
    prices: pl.DataFrame
    fundamentals: pl.DataFrame
    metadata: pl.DataFrame
```

Benefits:
- Single return value from the data loader.
- Functions take one argument instead of many.
- Easy to extend without breaking existing signatures.

---

### 6. Session State Discipline

Session state is Streamlit's mutable escape hatch. Use it deliberately:

| Good Use | Bad Use |
|---|---|
| Persisting user filter selections across re-runs | Storing computed DataFrames (use cache instead) |
| "Reset filters" callback via `st.session_state.update(...)` | Complex state machines with many interdependent keys |
| Tracking which items are selected in a multiselect | Anything that could be derived from widget return values |

**Clear Filters pattern:**
```python
def clear_filters():
    st.session_state["sector_filter"] = []
    st.session_state["portfolio_filter"] = []

st.sidebar.button("Clear Filters", on_click=clear_filters, type="primary")
```

---

### 7. Fragment Pattern for Performance

`@st.fragment()` lets a section re-run independently without re-running the entire page. Use it when:
- A heavy chart section sits below light filters.
- An interactive table shouldn't retrigger the full data pipeline on every row click.

```python
# Page-level: runs once, sets up data & sidebar
data = load_data()
filters = sidebar_widgets()

@st.fragment()
def render_dashboard(data, filters):
    # Only this block re-runs on widget interaction inside it
    chart = build_chart(data, filters)
    st.plotly_chart(chart)

render_dashboard(data, filters)
```

Place expensive data loading **above** the fragment. Lightweight rendering **inside** it.

**Note:** As the app grows, widget `key` collisions across fragments and pages can become a problem. No formal naming convention needed yet, but be aware of it.

---

### 8. Visual Consistency System

Define visual constants once. Reference everywhere.

#### Color Palette
Create a `Colors` class or module with named constants:
```python
class Colors:
    blue = "#2563eb"
    green = "#059669"
    red = "#dc2626"
    amber = "#d97706"

COLOR_SCALE_GREEN_RED = [Colors.green, Colors.light_green, Colors.yellow, Colors.light_red, Colors.red]
COLOR_SCALE_CONTRAST = [Colors.blue, Colors.orange, Colors.teal, Colors.purple, ...]
```

Use semantic scales (`GREEN_RED` for good/bad values) and categorical scales (`CONTRAST` for sectors, tickers).

**Lesson learned:** Pure yellow (`#FF0`) is invisible on white backgrounds. Use amber/gold instead.

#### Plotly Defaults
Standardize chart appearance:
- `template="plotly_white"` as the base.
- Remove redundant axis titles — if the chart title says "Revenue Growth", the y-axis doesn't need to repeat it.
- Consistent `height`, `margin`, and `legend` positioning.

#### Mapping Constants
Centralize emoji/icon mappings (sector → emoji, country → flag) in a `constants.py` file. Keeps views clean and mapping updates to one place.

---

### 9. Shared Components

Build a `views/common.py` for widgets that appear on multiple pages:

- **KPI cards** — `render_kpi_cards(metrics)` using `st.columns` + `st.metric`.
- **Empty states** — `render_empty_state(message, icon)` with a consistent look.
- **Sidebar header** — `render_sidebar_header(title, subtitle)` for brand consistency.
- **Selection widgets** — Portfolio/ticker selectors that encapsulate options logic.

These should be **thin wrappers** — if a component grows beyond ~40 lines, it probably belongs in its own file.

---

### 10. Error Handling — Keep It Simple

For local/private-use dashboards, let errors propagate naturally. Wrapping everything in broad `try/except` blocks adds unnecessary layers and hides the actual problem.

**Preferred approach:**
- Handle errors **locally** where it matters: if data for a specific chart is missing, skip that chart or show an empty state — don't crash the whole page.
- Let unexpected errors bubble up with a full traceback. During development, a visible traceback is more useful than a generic `st.error()` message.
- Use `st.stop()` for early exits when a required selection is missing (not as error handling, but as flow control).
- Use `loguru` for structured logging when debugging is needed.

```python
# Flow control — not error handling
if selected_portfolio is None:
    render_empty_state("Select a portfolio to continue")
    st.stop()

# Local graceful degradation
if df_chart_data.is_empty():
    st.info("No data available for this selection.")
else:
    render_chart(df_chart_data)
```

**Anti-pattern:** A big `try/except Exception` around the entire page that swallows all errors into `st.error()`. This makes debugging harder, not easier.

---

### 11. Project Configuration

#### Config File Strategy
- Use YAML for user-facing configuration (portfolios, watchlists, tickers).
- Validate with **Pydantic models** on load — fail fast with clear error messages.
- Use `pathlib.Path` throughout. Never `os.path.join`.

#### Entry Point
```bash
streamlit run src/app/main.py
```
Use a `Makefile` or similar for the launch command so nobody needs to remember the path.

---

### 12. File I/O Hygiene

For any app that writes data (ETL pipelines, user exports):
- **Atomic writes**: Write to `.tmp`, then rename. Prevents corruption on crash.
- **Parquet over CSV**: Type-safe, smaller, faster reads with Polars `scan_parquet`.
- Separate **data production** (ETL) from **data consumption** (Streamlit app). The app should never call external APIs directly.

---

### 13. Multi-Page App Structure

Streamlit's built-in multi-page system uses filename prefixes for ordering:

```
pages/
├── 01_Overview.py
├── 02_Asset_Detail.py
├── 03_Stock_Screener.py
└── 09_Admin.py
```

- Number prefixes control sidebar order.
- Each page calls `st.set_page_config()` at the top.
- Skip numbers (01, 02, 03, 09) to leave room for insertion.
- A `00_Startpage.py` or `main.py` serves as the landing page.

---

### 14. Testing Approach

For a project of this scope, comprehensive UI testing is overkill. The MVC pattern is the important part — it *enables* testing later without requiring it now.

- **Logic layer**: Testable with standard pytest — pure functions in, DataFrames out, no Streamlit dependency. This is where testing effort should go first when the time comes.
- **Views/Pages**: Visual QA for now. The MVC split means we can add `AppTest`-based integration tests later without restructuring.

---

## Part 2: Domain-Specific Learnings (Financial Dashboard)

### Data Model Choices
- **Long-format fundamentals** (`[date, metric, value, period]`) simplify aggregation (TTM calculations, metric pivoting) compared to wide tables.
- **Prices and fundamentals in separate Parquet stores**, partitioned by ticker. Keeps reads fast and schema evolution clean.
- Use a `MetricsEngine` to derive computed fields (P/E, ROCE, FCF Yield) at load time, not at render time.

### Financial UI Patterns
- **Strategy Factor Profiles**: Grouped bar charts comparing a stock's factor scores to sector reference values. Gives users immediate visual context.
- **Conditional table styling**: Color-code P/E ratios (green → red), ROCE (red → green), and data lag (fresh → stale). Maps readability to domain semantics.
- **Sparkline columns**: Inline 30-day price charts in the screener table for at-a-glance trend assessment.
- **Portfolio composition**: Sunburst charts for hierarchical breakdowns (Asset Class → Factor → Position).

### FX Handling
- A dedicated `FXEngine` converts all values to a target currency (EUR) using price data that's already in the local data lake.
- FX conversion happens in the logic layer, not in views.

### Portfolio Abstraction
- Portfolio types (`absolute`, `transactional`, `watchlist`) drive different calculation paths.
- Config-driven: portfolios defined in YAML, validated by Pydantic, consumed by portfolio engines. Adding a portfolio requires zero code changes.
