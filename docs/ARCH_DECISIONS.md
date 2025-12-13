# Architecture Decision Records (ADR)

## 001. Data Storage: Local Parquet
- **Decision:** Use local Parquet files partitioned by Ticker/Category.
- **Why:** Zero latency, easy generic usage with Polars, no database maintenance overhead.
- **Constraint:** All writes must be ATOMIC (Write Temp -> Rename) to prevent corruption during ETL crashes.

## 002. Data Processing: Polars
- **Decision:** All data transformation via Polars.
- **Why:** Performance on older hardware, clear syntax, strict typing. I want to learn more Polars!
- **Constraint:** No Pandas allowed in ETL layer.

## 003. Source Data: yfinance (Direct)
- **Decision:** Use `yfinance` library directly. Seems good enough.
- **Rejected:** OpenBB (too heavy/unstable for this scope), AlphaVantage (Rate limits).
- **Strategy:** Map raw yfinance dicts immediately to strict Pydantic models.

## 004. Fundamentals Schema: Long Format
- **Decision:** Store fundamentals as `[date, metric, value, period]`.
- **Why:** Simplifies aggregation (e.g., TTM calculation) significantly compared to wide tables with changing columns.
