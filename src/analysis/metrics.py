"""Financial metrics calculation engine using Polars.

Provides fundamental and valuation metrics for portfolio analysis.
"""

from typing import Any

import polars as pl
from loguru import logger

from src.analysis.ttm import TTMEngine


class MetricsEngine:
    """Calculates financial metrics from raw price and fundamental data.

    Implements fundamental ratio calculations and time-series valuation metrics
    using pure Polars expressions for performance.
    """

    def __init__(self) -> None:
        self.ttm_engine = TTMEngine()

    def _ensure_schema(self, df_to_check: pl.DataFrame, required_cols: list[str]) -> pl.DataFrame:
        """Ensure that all required columns exist in the DataFrame."""
        existing_cols = set(df_to_check.columns)
        missing_cols = [c for c in required_cols if c not in existing_cols]
        if missing_cols:
            exprs = []
            for c in missing_cols:
                dtype: Any
                # Heuristic to determine type based on column name
                if "date" in c:
                    dtype = pl.Date
                elif any(x in c for x in ["ticker", "currency", "period", "source"]):
                    dtype = pl.Utf8
                else:
                    dtype = pl.Float64

                exprs.append(pl.lit(None).cast(dtype).alias(c))

            df_to_check = df_to_check.with_columns(exprs)

        return df_to_check

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
        if df_fund.is_empty():
            logger.warning("Fundamentals DataFrame is empty, skipping fundamental metrics")
            return df_fund
        logger.info(f"Calculating fundamental metrics for {df_fund.height} records")
        if "currency" in df_fund.columns:
            df_fund = df_fund.pipe(self._pound_fix, value_cols=["cash_dividends_paid"])

        required_cols = [
            # fmt: off
            "ticker",
            # ROCE
            "total_assets",
            "total_current_liabilities",
            # Capital
            "total_equity",
            "total_debt",
            "long_term_debt",
            "short_term_debt",
            "cash_and_equivalents",
            # ROTCE
            "goodwill",
            "intangible_assets",
            "goodwill_and_intangible_assets",
            # FCF
            "operating_cash_flow",
            "capital_expenditure",
            "free_cash_flow",
            # Profitability / Margins
            "revenue",
            "gross_profit",
            "ebit",
            "net_income",
            "interest_expense",
            # fmt: on
        ]

        if "report_date" in df_fund.columns and "date" not in df_fund.columns:
            df_fund = df_fund.with_columns(pl.col("report_date").alias("date"))

        df_fund = self._ensure_schema(df_fund, required_cols)

        # Base metrics
        exprs = []
        # Debt
        debt_expr = pl.coalesce(
            pl.col("total_debt"),
            pl.col("long_term_debt").fill_null(0) + pl.col("short_term_debt").fill_null(0),
        )

        # Intangibles
        intangibles_expr = pl.coalesce(
            pl.col("goodwill_and_intangible_assets"),
            pl.col("goodwill").fill_null(0) + pl.col("intangible_assets").fill_null(0),
        )

        # Core Metrics
        capital_employed_expr = pl.col("total_assets") - pl.col("total_current_liabilities")

        # Capital Employed
        exprs.append(capital_employed_expr.alias("capital_employed"))

        tangible_cap_expr = (
            pl.col("total_equity")
            + debt_expr.fill_null(0)
            - pl.col("cash_and_equivalents").fill_null(0)
            - intangibles_expr.fill_null(0)
        )
        exprs.append(tangible_cap_expr.alias("tangible_capital_employed"))

        # Net Debt
        exprs.append(
            (debt_expr.fill_null(0) - pl.col("cash_and_equivalents").fill_null(0)).alias("net_debt")
        )

        # Free Cash Flow (calculated)
        fcf_calculated = pl.col("operating_cash_flow") - pl.col("capital_expenditure").abs()
        exprs.append(fcf_calculated.alias("fcf_calculated"))

        exprs.append(
            pl.coalesce(pl.col("free_cash_flow"), fcf_calculated).alias("free_cash_flow_final")
        )

        # Ratios

        # ROCE
        exprs.append((pl.col("ebit") / capital_employed_expr).alias("roce"))
        # ROTCE
        exprs.append(
            pl.when(tangible_cap_expr.gt(0))
            .then(pl.col("ebit") / tangible_cap_expr)
            .otherwise(None)
            .alias("rotce")
        )
        # Net Debt to EBIT
        exprs.append((debt_expr / pl.col("ebit")).alias("net_debt_to_ebit"))
        # Interest Coverage
        exprs.append((pl.col("ebit") / pl.col("interest_expense").abs()).alias("interest_coverage"))
        # Margins
        exprs.append((pl.col("net_income") / pl.col("revenue")).alias("net_profit_margin"))
        exprs.append((pl.col("gross_profit") / pl.col("revenue")).alias("gross_margin"))
        exprs.append((pl.col("ebit") / pl.col("revenue")).alias("ebit_margin"))

        df_fund = (
            df_fund.with_columns(exprs)
            .with_columns(pl.col("free_cash_flow_final").alias("free_cash_flow"))
            .with_columns(
                # Cash Conversion Ratio
                (pl.col("free_cash_flow") / pl.col("net_income")).alias("cash_conversion_ratio")
            )
            .drop("free_cash_flow_final")
        )
        return df_fund

    def _pound_fix(self, df: pl.DataFrame, value_cols: list[str] | None = None) -> pl.DataFrame:
        """
        There are two problems with LSE data:
        1. Prices are in pence, need to convert to pounds
        2. Currency column is 'GBp' but should be 'GBP'
        """
        expr = []
        for col in value_cols or []:
            expr.append(
                pl.when((pl.col("currency") == "GBp") & (pl.col("ticker").str.ends_with(".L")))
                .then(pl.col(col) / 100)
                .otherwise(pl.col(col))
                .alias(col)
            )
        expr.append(
            pl.when(pl.col("currency") == "GBp")
            .then(pl.lit("GBP"))
            .otherwise(pl.col("currency"))
            .alias("currency")
        )
        return df.with_columns(expr)

    def calculate_valuation_metrics(
        self,
        df_prices: pl.DataFrame,
        df_annual: pl.DataFrame,
        df_quarterly: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """
        Calculate daily valuation metrics by merging prices and fundamentals.

        Strategy: HYBRID MERGE
        1. Calculate TTM data from quarterly reports (if available).
        2. Merge both Annual and TTM data onto the price history (asof join).
        3. Calculate ratios prioritizing TTM data, falling back to Annual data.

        Args:
            df_prices: Daily price history.
            df_annual: Annual financial reports.
            df_quarterly: Quarterly financial reports (optional).

        Returns:
            DataFrame with added valuation columns.
        """
        logger.info(f"Calculating valuation metrics for {df_prices.height} price records")
        if df_prices.is_empty():
            logger.warning("Price DataFrame is empty, skipping valuation metrics")
            return df_prices

        # Before we do anything, prices from London Stock Exchange (LSE) need special handling
        # They are reported in pence, so we convert to pounds here
        # when currency GBP and ticker ends with .L
        if "currency" in df_prices.columns:
            df_prices = df_prices.pipe(
                self._pound_fix,
                value_cols=["open", "high", "low", "close", "adj_close"],
            )

        # A. Annual Data
        if not df_annual.is_empty():
            df_annual = self._ensure_schema(
                df_annual,
                [
                    # fmt: off
                    "ticker",
                    "report_date",
                    "diluted_eps",
                    "diluted_average_shares",
                    "revenue",
                    "free_cash_flow",
                    "basic_average_shares",
                    "cash_dividends_paid",
                    # fmt: on
                ],
            )
            q_annual = df_annual.select(
                [
                    pl.col("ticker"),
                    pl.col("report_date"),
                    pl.col("diluted_eps").alias("eps_annual"),
                    pl.col("revenue").alias("revenue_annual"),
                    pl.col("free_cash_flow").alias("fcf_annual"),
                    # Prefer diluted for conservative valuation, fallback to basic if missing
                    pl.coalesce(
                        pl.col("diluted_average_shares"), pl.col("basic_average_shares")
                    ).alias("shares_annual"),
                    # We also need dividends for yield calculation
                    pl.col("cash_dividends_paid").abs().alias("dividend_annual"),
                ]
            ).sort("report_date")
        else:
            q_annual = pl.DataFrame()
        # B. TTM Data
        if df_quarterly is not None and not df_quarterly.is_empty():
            df_quarterly = self._ensure_schema(
                df_quarterly,
                [
                    # fmt: off
                    "ticker",
                    "report_date",
                    "diluted_eps",
                    "diluted_average_shares",
                    "revenue",
                    "free_cash_flow",
                    "share_issued",
                    # fmt: on
                ],
            )
            ttm_tmp = self.ttm_engine.calculate_ttm_history(df_quarterly)
            q_ttm = ttm_tmp.select(
                [
                    pl.col("ticker"),
                    pl.col("report_date"),
                    # Rename to avoid collision
                    pl.col("net_income_ttm").alias("net_income_ttm"),
                    pl.col("diluted_eps_ttm").alias("eps_ttm"),
                    pl.col("revenue_ttm"),
                    pl.col("free_cash_flow_ttm").alias("fcf_ttm"),
                    # Prefer diluted TTM
                    pl.coalesce(
                        pl.col("share_issued_ttm"),
                        pl.col("diluted_average_shares_ttm"),
                        pl.col("basic_average_shares_ttm"),
                    ).alias("shares_ttm"),
                ]
            ).sort("report_date")
        else:
            q_ttm = pl.DataFrame()

        df_p = df_prices.sort(["ticker", "date"])
        if not q_annual.is_empty():
            df_p = df_p.join_asof(
                q_annual,
                left_on="date",
                right_on="report_date",
                by="ticker",
                strategy="backward",
            )
        if not q_ttm.is_empty():
            df_p = df_p.join_asof(
                q_ttm,
                left_on="date",
                right_on="report_date",
                by="ticker",
                strategy="backward",
                suffix="_ttm",
            )
        # Important Ensure Schema Consistency
        expected_ttm_cols = [
            "eps_ttm",
            "revenue_ttm",
            "fcf_ttm",
            "shares_ttm",
            "report_date_ttm",
        ]
        df_p = self._ensure_schema(df_p, expected_ttm_cols)
        expected_annual_cols = [
            "eps_annual",
            "revenue_annual",
            "fcf_annual",
            "shares_annual",
            "dividend_annual",
            "report_date",
        ]
        df_p = self._ensure_schema(df_p, expected_annual_cols)

        eps_expr = pl.coalesce(pl.col("eps_ttm"), pl.col("eps_annual"))
        report_date_expr = pl.coalesce(pl.col("report_date_ttm"), pl.col("report_date"))
        shares_expr = pl.coalesce(
            pl.col("shares_ttm"),
            pl.col("shares_annual"),
            pl.col("shares") if "shares" in df_p.columns else pl.lit(None),
        )

        rev_expr = pl.coalesce(pl.col("revenue_ttm"), pl.col("revenue_annual"))
        rps_expr = rev_expr / shares_expr

        fcf_val_expr = pl.coalesce(pl.col("fcf_ttm"), pl.col("fcf_annual"))
        fcfps_expr = fcf_val_expr / shares_expr

        df_enriched = df_p.with_columns(
            [
                # --- Valuation Ratios ---
                (pl.col("close") / eps_expr).alias("pe_ratio"),
                (pl.col("close") / rps_expr).alias("ps_ratio"),
                (fcfps_expr / pl.col("close")).alias("fcf_yield"),
                # Dividend Yield (Using Annual Dividend for safety)
                (pl.col("dividend_annual") / shares_expr / pl.col("close")).alias("div_yield_calc"),
                # --- Data Quality / Metadata ---
                pl.when(pl.col("eps_ttm").is_not_null())
                .then(pl.lit("TTM"))
                .when(pl.col("eps_annual").is_not_null())
                .then(pl.lit("Annual"))
                .otherwise(pl.lit("N/A"))
                .alias("valuation_source"),
                report_date_expr.alias("metric_date"),
                shares_expr.alias("diluted_average_shares"),
            ]
        ).with_columns(
            [(pl.col("date") - pl.col("metric_date")).dt.total_days().alias("data_lag_days")]
        )
        # also add dividends

        if "dividend" in df_enriched.columns:
            df_enriched = df_enriched.sort(["ticker", "date"])
            df_div_rolling = (
                df_enriched.select(["ticker", "date", "dividend"])
                .with_columns(pl.col("dividend").fill_null(0))
                .rolling(
                    index_column="date",
                    period="1y",
                    group_by="ticker",
                    closed="right",
                )
                .agg(pl.col("dividend").sum().alias("rolling_dividend_sum"))
            )
            df_enriched = df_enriched.join(
                df_div_rolling, on=["ticker", "date"], how="left"
            ).with_columns(
                (pl.col("rolling_dividend_sum") / pl.col("close")).alias("dividend_yield")
            )
        else:
            df_enriched = df_enriched.with_columns(
                pl.lit(0).alias("rolling_dividend_sum"),
                pl.lit(0).alias("dividend_yield"),
            )

        return df_enriched

    def calculate_fair_value_history(
        self,
        df_prices: pl.DataFrame,
        df_fundamentals: pl.DataFrame,
        years: int = 5,
    ) -> pl.DataFrame:
        """
        Calculate historical Fair Value based on the 5-year median P/E ratio.

        Logic:
        1. Determine the time window (last 'years' years from max date).
        2. Calculate the median P/E ratio for each ticker within that window
         . ignoring negative PEs).
        3. Fair Value = Current TTM EPS * Median 5y P/E.
        """
        if "pe_ratio" not in df_prices.columns:
            logger.warning("Price DataFrame lacks 'pe_ratio', skipping fair value calc")
            return df_prices

        try:
            max_date = df_prices["date"].max()
            if max_date is None:
                logger.warning("Price DataFrame has no valid dates, skipping fair value calc")
                return df_prices
            start_date = max_date - pl.duration(days=years * 365)
        except Exception as e:
            logger.error(f"Error determining max date in prices: {e}")
            start_date = pl.datetime(2021, 1, 1)

        pe_stats = (
            df_prices.filter(
                (pl.col("date") >= start_date)
                & (pl.col("pe_ratio").gt(0))
                # filter out extreme outliers
                # (increased to 250 as ridiculous 150 can be valid e.g. Tesla or high growth)
                & (pl.col("pe_ratio").lt(250))
            )
            .group_by("ticker")
            .agg(pl.col("pe_ratio").median().alias("median_pe"))
        )
        df_fair = (
            df_prices.join(pe_stats, on="ticker", how="left")
            .with_columns(
                (pl.col("close") / pl.col("pe_ratio")).alias("implied_eps"),
            )
            .with_columns((pl.col("implied_eps") * pl.col("median_pe")).alias("fair_value"))
        )

        if not df_fair.is_empty():
            sample = df_fair.select(["ticker", "date", "close", "fair_value", "median_pe"]).tail(5)
            logger.debug(f"Sample fair value calculations:\n{sample.to_dicts()}")
        return df_fair

    def calculate_growth_metrics(
        self,
        df_fundamentals: pl.DataFrame,
        metric_columns: list[str],
        period: int = 1,
    ) -> pl.DataFrame:
        """Calculate period-over-period growth rates for specified metrics.

        Computes g(i) = v(i) / v(i-period) for each ticker separately.
        Sorts data by ticker and date to ensure correct temporal ordering.
        """
        # Sort by ticker and date to ensure shift operates on correct temporal order
        df_sorted = df_fundamentals.sort(["ticker", "report_date"])

        growth_cols = [
            (((pl.col(c) / pl.col(c).shift(period)) - 1).over("ticker")).alias(f"{c}_growth")
            for c in metric_columns
        ]

        return df_sorted.with_columns(growth_cols)
