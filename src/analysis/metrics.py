"""Financial metrics calculation engine using Polars.

Provides fundamental and valuation metrics for portfolio analysis.
"""

import polars as pl
from loguru import logger


class MetricsEngine:
    """Calculates financial metrics from raw price and fundamental data.

    Implements fundamental ratio calculations and time-series valuation metrics
    using pure Polars expressions for performance.
    """

    def calculate_fundamental_metrics(self, df_fund: pl.DataFrame) -> pl.DataFrame:
        """Add calculated fundamental metrics to raw fundamentals data.

        Computes capital efficiency and leverage metrics using Polars expressions.
        Handles missing columns gracefully by conditionally calculating metrics.

        Args:
            df_fund: Raw fundamentals DataFrame with yearly granularity.
                Expected columns: ticker, date, ebit, total_assets,
                total_current_liabilities, long_term_debt, cash_and_equivalents,
                operating_cash_flow, capital_expenditure, basic_average_shares.

        Returns:
            DataFrame with added columns:
                - capital_employed
                - roce
                - free_cash_flow
                - net_debt
                - interest_coverage (if interest_expense exists)
        """
        logger.info(f"Calculating fundamental metrics for {df_fund.height} records")

        cols_available = set(df_fund.columns)

        # We need to prepare them to be used in further calculations
        fundamental_metrics = df_fund.with_columns(
            # Capital Employed = Total Assets - Current Liabilities
            (pl.col("total_assets") - pl.col("total_current_liabilities")).alias(
                "capital_employed"
            ),
            # adding date as alias for consistency in later joins
            pl.col("report_date").alias("date"),
        )

        expr_list = []

        # ROCE = EBIT / Capital Employed (handle division by zero)
        expr_list.append((pl.col("ebit") / pl.col("capital_employed")).fill_nan(None).alias("roce"))

        # Free Cash Flow: use existing column or calculate
        if "free_cash_flow" in cols_available:
            logger.debug("Using existing free_cash_flow column")
        else:
            expr_list.append(
                (pl.col("operating_cash_flow") - pl.col("capital_expenditure")).alias(
                    "free_cash_flow"
                )
            )

        # Net Debt = Long Term Debt - Cash
        expr_list.append(
            (pl.col("long_term_debt") - pl.col("cash_and_equivalents")).alias("net_debt")
        )

        # Interest Coverage = EBIT / Interest Expense (conditional)
        if "interest_expense" in cols_available:
            logger.debug("Calculating interest_coverage")
            expr_list.append(
                (pl.col("ebit") / pl.col("interest_expense"))
                .fill_nan(None)
                .alias("interest_coverage")
            )
        else:
            logger.debug("interest_expense not available, skipping interest_coverage")

        result = fundamental_metrics.with_columns(expr_list)

        logger.info(f"Added {len(expr_list)} fundamental metrics")
        return result

    def calculate_valuation_metrics(
        self,
        df_prices: pl.DataFrame,
        df_fund_enriched: pl.DataFrame,
    ) -> pl.DataFrame:
        """Calculate daily valuation metrics by merging prices and fundamentals.

        Performs three steps:
        1. Calculate rolling 12M dividend yield
        2. Time-travel join to map fundamentals to each price date
        3. Compute market-cap based valuation ratios

        Args:
            df_prices: Daily price data with columns: ticker, date, close, dividend, currency.
            df_fund_enriched: Enriched fundamentals from calculate_fundamental_metrics().

        Returns:
            Daily DataFrame with price, dividend_yield, market_cap, fcf_yield,
            net_debt_ebitda, and all joined fundamental metrics.
        """
        logger.info(f"Calculating valuation metrics for {df_prices.height} price records")

        # Step A: Calculate Rolling 12M Dividend Yield
        logger.debug("Step A: Calculating rolling dividend yield")

        # Ensure sorted by ticker and date for rolling window
        df_prices_sorted = df_prices.sort(["ticker", "date"])

        df_with_div_yield = df_prices_sorted.with_columns(
            [
                # Rolling sum of dividends over 365 days
                pl.col("dividend")
                .rolling_sum_by(by="date", window_size="365d", closed="right")
                .over("ticker")
                .alias("rolling_dividend_sum"),
            ]
        ).with_columns(
            [
                # Dividend yield = rolling sum / current price
                (pl.col("rolling_dividend_sum") / pl.col("close"))
                .fill_nan(None)
                .alias("dividend_yield")
            ]
        )

        # Step B: Time-Travel Join (join_asof)
        logger.debug("Step B: Performing time-travel join with fundamentals")

        # Ensure fundamentals are sorted by date for join_asof
        df_fund_sorted = df_fund_enriched.sort(["ticker", "date"])

        df_merged = df_with_div_yield.join_asof(
            df_fund_sorted,
            on="date",
            by="ticker",
            strategy="backward",
        )

        # Step C: Calculate Valuation KPIs
        logger.debug("Step C: Calculating valuation KPIs")

        intermediate_cols = []
        valuation_cols = []

        # Market Cap = Price * Shares Outstanding
        if "basic_average_shares" in df_merged.columns:
            intermediate_cols.append(
                (pl.col("close") * pl.col("basic_average_shares")).alias("market_cap")
            )

            # FCF Yield = Free Cash Flow / Market Cap
            if "free_cash_flow" in df_merged.columns:
                valuation_cols.append(
                    (pl.col("free_cash_flow") / pl.col("market_cap"))
                    .fill_nan(None)
                    .alias("fcf_yield")
                )

        # Net Debt / EBITDA (using EBIT as proxy)
        if "net_debt" in df_merged.columns and "ebit" in df_merged.columns:
            valuation_cols.append(
                (pl.col("net_debt") / pl.col("ebit")).fill_nan(None).alias("net_debt_ebitda")
            )

        result = df_merged.with_columns(intermediate_cols).with_columns(valuation_cols)

        logger.info(
            f"Valuation metrics calculated: {result.height} records, "
            f"{len(valuation_cols)} additional metrics"
        )

        return result
