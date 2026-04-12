# Polars Metrics Engine — Patterns & Learnings

Learnings from building a financial metrics calculation pipeline in Polars. Covers patterns for time series enrichment, TTM calculations, multi-currency handling, and where Polars gives practical advantages over Pandas.

---

## The General Idea

Financial data arrives as two separate time series — **daily prices** and **periodic fundamentals** (annual/quarterly reports). The core challenge is combining them into a single enriched dataset where every price row carries its most recent fundamental context.

The engine follows a three-stage pipeline:

```
Raw Fundamentals  →  Calculated Ratios  →  Merged onto Price History
  (long-format)      (ROCE, FCF, etc.)     (via asof join)
```

This happens once at load time (cached), not at render time. The Streamlit app only reads the enriched result.

---

## Architecture: Engine Classes

The calculation logic is split into specialized engines, each owning one concern:

| Engine | Responsibility | Key Pattern |
|---|---|---|
| `TTMEngine` | Roll quarterly data into trailing-12-month figures | Rolling window aggregation |
| `MetricsEngine` | Calculate fundamental ratios + merge onto prices | Expression lists, `join_asof` |
| `FXEngine` | Currency conversion using historical rates | Split-process-concat |
| `PortfolioEngine` | Portfolio value history from positions × prices | Cumulative sums, `join_asof` |
| `StrategyEngine` | Factor scoring per stock | Pydantic models, weighted expressions |

Each engine is a plain Python class with no Streamlit dependency — instantiated in the logic layer, results consumed by views.

---

## Polars Patterns

### 1: Expression Lists

The most distinctive Polars pattern in this project. Instead of calculating metrics one by one (mutating the DataFrame repeatedly), build a list of expressions and apply them in a single `.with_columns()`:

```python
exprs = []

# Each expression is self-contained
exprs.append((pl.col("total_assets") - pl.col("total_current_liabilities")).alias("capital_employed"))
exprs.append((pl.col("ebit") / capital_employed_expr).alias("roce"))
exprs.append((pl.col("gross_profit") / pl.col("revenue")).alias("gross_margin"))
exprs.append((pl.col("ebit") / pl.col("revenue")).alias("ebit_margin"))

df = df.with_columns(exprs)
```

**Why it works well:**
- All expressions run in a single pass — Polars parallelizes them internally.
- Easy to extend: adding a new metric is one `append` line.
- Readable: each expression is named and self-contained.
- Expressions can reference each other within the same batch if defined carefully (or chain multiple `.with_columns()` calls when there are dependencies).

**Pandas equivalent would be:** Repeated `df["new_col"] = df["a"] / df["b"]` mutations, or a messy `.assign()` chain. No parallelization.

---

### 2: `pl.coalesce` for Fallback Chains

Financial data is messy. A field might exist in one source but not another. `coalesce` picks the first non-null value — like SQL `COALESCE`:

```python
# Prefer TTM EPS, fall back to annual
eps = pl.coalesce(pl.col("eps_ttm"), pl.col("eps_annual"))

# Prefer diluted shares, fall back to basic
shares = pl.coalesce(
    pl.col("diluted_average_shares"),
    pl.col("basic_average_shares"),
)

# Debt: use total if available, otherwise reconstruct
debt = pl.coalesce(
    pl.col("total_debt"),
    pl.col("long_term_debt").fill_null(0) + pl.col("short_term_debt").fill_null(0),
)
```

This pattern appears everywhere in the metrics engine. It keeps the code declarative — you state *what you prefer*, not *how to check* for nulls.

**Pandas equivalent:** Nested `np.where` or chained `.fillna()` calls. Much noisier.

---

### 3: `join_asof` — The Financial Time Series Join

The most important join in the project. Prices are daily, fundamentals are quarterly/annual. `join_asof` with `strategy="backward"` maps each price row to the most recent available fundamental report:

```python
df_prices = df_prices.join_asof(
    df_annual,
    left_on="date",
    right_on="report_date",
    by="ticker",
    strategy="backward",
)
```

This means: for each `(ticker, date)` in prices, find the most recent `report_date ≤ date` in fundamentals and attach that row.

**Used for:**
- Mapping annual/TTM EPS onto daily prices → P/E ratio history.
- Mapping FX rates onto asset prices → currency conversion.
- Mapping transactions onto trading days → portfolio positions.

**Pandas equivalent:** Manual `pd.merge_asof` — similar API, but Polars is significantly faster on large datasets and handles the `by` grouping more naturally.

---

### 4: TTM via Rolling Windows

Trailing Twelve Months is the standard for fresh valuations. The engine uses Polars' `.rolling()` with a date-based window:

```python
df_ttm = (
    df_quarterly
    .sort(["ticker", "report_date"])
    .rolling(
        index_column="report_date",
        period="395d",          # ~13 months: safely captures 4 quarters
        group_by="ticker",
        closed="right",
    )
    .agg([
        # Flow metrics (P&L, Cash Flow): sum of last 4 quarters
        pl.col("revenue").tail(4).sum().alias("revenue_ttm"),
        pl.col("free_cash_flow").tail(4).sum().alias("fcf_ttm"),
        # Point metrics (Balance Sheet): latest snapshot
        pl.col("total_debt").last().alias("total_debt_ttm"),
        # Quality gate
        pl.len().alias("_record_count"),
    ])
    .filter(pl.col("_record_count") >= 4)  # Only emit when we have a full year
)
```

**Key decisions:**
- **395 days**, not 365: reporting dates shift slightly between quarters. The extra buffer prevents data loss.
- **`tail(4).sum()`** instead of just `.sum()`: ensures we only sum the 4 most recent quarters within the window, not accidental extras.
- **`_record_count >= 4`** quality gate: prevents publishing TTM from 2 quarters (which would understate revenue and overstate valuations).
- **Flow vs. Point distinction**: Revenue gets summed (it accumulates). Debt gets `.last()` (it's a snapshot).

**This would be painful in Pandas.** `pd.DataFrame.rolling()` doesn't support grouped date-based windows natively. You'd need `groupby().apply()` with a custom function — slow and verbose.

---

### 5: Hybrid Merge (TTM + Annual Fallback)

Not every ticker has quarterly data. The engine merges both sources onto prices, then uses `coalesce` to prefer TTM where available:

```
Prices  ←asof join←  Annual fundamentals  (always available)
        ←asof join←  TTM fundamentals     (when quarterly data exists)
```

```python
# After both joins:
eps = pl.coalesce(pl.col("eps_ttm"), pl.col("eps_annual"))
pe_ratio = pl.col("close") / eps

# Track which source was used
pl.when(pl.col("eps_ttm").is_not_null())
  .then(pl.lit("TTM"))
  .when(pl.col("eps_annual").is_not_null())
  .then(pl.lit("Annual"))
  .otherwise(pl.lit("N/A"))
  .alias("valuation_source")
```

The `valuation_source` column is surfaced in the UI so the user knows whether they're looking at fresh TTM or older annual data.

---

### 6: `_ensure_schema` — Defensive Column Handling

Data providers change schemas. A column that existed last quarter might disappear. Instead of crashing, the engine fills missing columns with typed nulls:

```python
def _ensure_schema(self, df: pl.DataFrame, required_cols: list[str]) -> pl.DataFrame:
    existing = set(df.columns)
    missing = [c for c in required_cols if c not in existing]
    if missing:
        exprs = [pl.lit(None).cast(inferred_type).alias(c) for c in missing]
        df = df.with_columns(exprs)
    return df
```

Called before every major calculation step. This means downstream expressions can always reference the expected columns — they'll just be null if the data wasn't there. `coalesce` then handles the fallback.

---

### 7: `pipe()` for Composable Transformations

Instead of deeply nested function calls, chain transformations with `.pipe()`:

```python
df_prices_latest = (
    df_prices
    .filter(pl.col("ticker").is_in(selected_tickers))
    .pipe(fx_engine.convert_multiple_to_target, amount_cols=["close", "fair_value"], source_currency_col="currency")
    .sort(["ticker", "date"])
    .group_by("ticker")
    .agg(...)
)
```

`.pipe()` lets you insert a multi-argument function (like FX conversion) into a method chain without breaking the flow. The DataFrame is passed as the first argument.

---

### 8: Split-Process-Concat for Multi-Currency Data

The FX engine can't apply a single rate to all rows (different currencies need different rates). Solution: split into groups, process each, recombine:

```python
# Home currency rows: no conversion needed
df_home = df.filter(pl.col("currency") == "EUR")

# Foreign currency rows: join FX rate, then multiply
for currency, rate_df in self.fx_rates.items():
    df_chunk = df.filter(pl.col("currency") == currency)
    df_chunk = df_chunk.join_asof(rate_df, on="date", strategy="backward")
    chunks.append(df_chunk.with_columns(pl.col(amount_col) / pl.col("rate")))

df_result = pl.concat([df_home, *chunks], how="vertical_relaxed")
```

`how="vertical_relaxed"` handles slight schema differences between chunks (e.g., one chunk might not have a `rate` column).

---

### 9: Aggregation with Embedded Lists

For the screener's sparkline columns, Polars' native `List` type stores 30 data points in a single cell:

```python
.group_by("ticker").agg(
    pl.last("close_EUR").alias("close"),
    pl.tail("close_EUR", 30).alias("close_30d"),  # List[Float64] in one cell
    pl.col("pe_ratio").quantile(0.25).alias("pe_p25"),
)
```

The view layer reads `close_30d` directly as a list and renders an inline sparkline. No second query, no separate DataFrame.

---

## Where Polars Shines Over Pandas (In This Project)

| Area | Polars Advantage | Pandas Pain |
|---|---|---|
| **`join_asof` with grouping** | Native `by="ticker"` + `strategy="backward"` in one call | `merge_asof` exists but is slower, grouping less ergonomic |
| **Rolling windows by date + group** | `.rolling(index_column=, group_by=)` in one pass | `groupby().apply()` + custom rolling — slow, verbose |
| **Expression parallelism** | 15 expressions in one `.with_columns()` run in parallel | Sequential `df["col"] = ...` mutations |
| **`coalesce`** | Clean declarative fallback chains | Nested `.fillna()` or `np.where` |
| **Null handling** | First-class null propagation through expressions | `NaN` vs `None` confusion, `fillna` vs `replace` |
| **Type safety** | Schema errors caught at expression build time | Runtime `KeyError` or silent dtype coercion |
| **`vertical_relaxed` concat** | Schema evolution handled gracefully | `pd.concat` requires matching columns or throws |
| **List columns** | Native `List[Float64]` type for sparklines | Requires storing as object dtype (slow, untyped) |

The strongest practical win is the **expression model**. Building a list of 15 metric calculations and applying them in one `.with_columns()` is cleaner and faster than any Pandas approach. For time series work with irregular joins, `join_asof` and date-based `.rolling()` are category advantages.

---

## Domain-Specific Insights

**Data quality is the real engineering problem.** The metrics engine spends ~40% of its code handling missing data, schema drift, and source-specific quirks (LSE pence-to-pounds conversion, EPS fallback chains). The actual ratio math is trivial.

**The TTM engine is the highest-value component.** Annual data can be 6–12 months stale. TTM keeps valuations current. The `flow_metrics` vs. `point_metrics` distinction (sum vs. last) is critical to get right — summing balance sheet items is a common bug.

**`data_lag_days` is a key UX insight.** Every enriched price row carries `(date - metric_date).dt.total_days()` — the staleness of its fundamental data. This is surfaced as a colored column in the screener so users immediately see which tickers have fresh data and which are coasting on old reports.

**The `_pound_fix` is a real-world wart.** London Stock Exchange prices come in pence, not pounds. A conditional `pl.when().then()` expression fixes this at the boundary. Domain-specific edge cases like this are inevitable — handle them early and document why.

**Coalescing and fallback chains are the unsung hero.** Financial quantities often have multiple computation routes (e.g., EPS from the API vs. derived from net income / shares). `pl.coalesce` plus windowed aggregations make it natural to define a preference order without nested if-else logic. Combined with `_ensure_schema`, this means the engine gracefully degrades when a data source changes — instead of crashing, it falls through to the next available path.

---

## Notes & Deferred Decisions

**Expression composition via variable reuse.** When metric B depends on metric A (e.g., ROCE = EBIT / capital_employed), the cleanest Polars pattern is to define intermediate expressions as variables and compose them:

```python
capital_employed = pl.col("total_assets") - pl.col("total_current_liabilities")
roce = pl.col("ebit") / capital_employed

df = df.with_columns([
    capital_employed.alias("capital_employed"),
    roce.alias("roce"),
])
```

This avoids chained `.with_columns()` calls and keeps everything in a single pass. The project uses this in some places but not consistently — worth adopting as the default style going forward.

**Testing the metrics layer** is deferred. The engines are pure Polars with no Streamlit dependency, so they're straightforward to test with pytest when the time comes. No decision needed yet on fixture strategy (synthetic DataFrames vs. real Parquet snapshots).

**Premature optimization is the enemy.** The TTM 395-day window, `_ensure_schema` heuristics, engine nesting, and full-pipeline enrichment on cache miss all work well at current scale. Don't optimize what isn't slow.
