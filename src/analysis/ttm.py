"""TTM (Trailing Twelve Months) Calculation Engine.

Transforms raw quarterly financial data into annualized metrics using rolling windows.
Crucial for valuation metrics (P/E, P/S) to avoid seasonality and outdated annual data.
"""

import polars as pl
from loguru import logger


class TTMEngine:
    """Calculates Trailing Twelve Months (TTM) metrics from quarterly data."""

    def __init__(self) -> None:
        # Flow Metrics: Values that accumulate over time (Income Statement, Cash Flow)
        # Logic: Sum of the last 4 quarters
        self.flow_metrics = [
            "revenue",
            "gross_profit",
            "ebit",
            "net_income",
            "operating_cash_flow",
            "capital_expenditure",
            "free_cash_flow",
            "interest_expense",
            "cash_dividends_paid",
            "diluted_eps",
        ]

        # Point Metrics: Snapshot values at a specific date (Balance Sheet)
        # Logic: Take the value of the last available quarter
        self.point_metrics = [
            "total_assets",
            "total_current_liabilities",
            "total_equity",
            "long_term_debt",
            "short_term_debt",
            "total_debt",
            "cash_and_equivalents",
            "basic_average_shares",
            "diluted_average_shares",
        ]

    def calculate_ttm_history(self, df_quarterly: pl.DataFrame) -> pl.DataFrame:
        """Calculate TTM history for all tickers in the dataset.

        Args:
            df_quarterly: Polars DataFrame containing raw quarterly reports.
            Must contain ['ticker', 'report_date'] columns.

        Returns:
            DataFrame with TTM-adjusted metrics, sorted by date.
        """
        if df_quarterly is None or df_quarterly.is_empty():
            logger.warning("TTM Engine received empty dataframe.")
            return pl.DataFrame()

        initial_count = df_quarterly.height
        df_clean = df_quarterly.filter(
            pl.col("revenue").is_not_null() | pl.col("net_income").is_not_null()
        )
        dropped_count = initial_count - df_clean.height
        if dropped_count > 0:
            logger.info(
                f"Dropped {dropped_count} quarterly rows with no P&L data for TTM calculation. "
                "(Balance Sheet only rows cannot contribute to TTM metrics.)"
            )
        if df_clean.is_empty():
            logger.warning("No valid quarterly data available for TTM calculation.")
            return pl.DataFrame()

        logger.info(f"Calculating TTM metrics for {df_clean['ticker'].n_unique()} tickers...")

        # 1. Prepare Data
        # Ensure strict sorting for rolling window functions
        df_raw = df_clean.sort(["ticker", "report_date"])

        # 2. Define Aggregations
        # We process tickers in parallel using over("ticker")
        aggs = []

        # A. Flow Metrics: Rolling Sum (Window = 4 periods)
        # min_periods=4 ensures we don't calculate TTM
        # if we only have 1 quarter (would look huge/tiny)
        # For strict data quality, we require 4 quarters.
        # (Relax to 3 or 2 if data is sparse, but risky for seasonality).
        for col in self.flow_metrics:
            if col in df_raw.columns:
                aggs.append(pl.col(col).tail(4).sum().alias(f"{col}_ttm"))

        # B. Point Metrics: Last Value
        # Technically, the raw value IS the value at that date.
        # But we alias it for consistency in the merging layer later.
        for col in self.point_metrics:
            if col in df_raw.columns:
                aggs.append(pl.col(col).last().alias(f"{col}_ttm"))

        # C. Metadata passthrough (keep original date)
        # TTM date is usually the date of the last quarter used.
        aggs.append(pl.col("currency").last())
        aggs.append(pl.col("period_type").last())
        aggs.append(pl.len().alias("_record_count"))  # For debugging / quality checks

        # 3. Execute Calculation
        try:
            df_ttm = (
                df_raw.sort(["ticker", "report_date"])
                .rolling(
                    index_column="report_date",
                    period="395d",
                    group_by="ticker",
                    closed="right",
                )
                .agg(aggs)
                .sort(["ticker", "report_date"])
                .filter(pl.col("_record_count") >= 4)  # Ensure full 4 quarters
                .drop("_record_count")
            )

            return df_ttm

        except Exception as e:
            logger.error(f"TTM Calculation failed: {e}")
            return pl.DataFrame()
