from typing import Any

import polars as pl

from src.analysis.fx import FXEngine
from src.app.logic.data_loader import DashboardData
from src.app.logic.entry import calculate_volatility_metrics

METRIC_DESCRIPTIONS = {
    "timing": {
        "z_score": (
            "Tactical deviation: (Close - SMA_50) / STD_50. " "Measures short-term overextension."
        ),
        "dist_200_pct": (
            "Structural trend: % distance to 200-day SMA. " ">0 indicates uptrend context."
        ),
        "vola_annual_pct": "Annualized volatility based on 200-day window of daily returns.",
    },
    "valuations": {
        "fair_value": (
            "Intrinsic value estimate: Current TTM EPS * 5-Year Median P/E "
            "(excluding outliers > 250)."
        ),
        "pegy_ratio": (
            "Valuation/Growth adjustment: Forward P/E / "
            "(Implied EPS Growth % + Dividend Yield %)."
        ),
        "fcf_yield": (
            "Free Cash Flow per share / Price. "
            "Measure of cash generation relative to market cap."
        ),
        "implied_eps_growth": "Market implied growth: (Forward EPS / Trailing EPS) - 1.",
    },
    "fundamentals": {
        "rule_40": (
            "SaaS Efficiency: EBITDA Margin % + Revenue Growth %. "
            ">40 is widely considered elite."
        ),
        "roce": "Return on Capital Employed. Measures efficiency of capital allocation.",
        "net_debt_to_ebitda": (
            "Leverage ratio. Higher values indicate higher debt relative to earnings."
        ),
    },
    "sources": {
        "ttm_vs_annual": (
            "Metrics prioritize Trailing Twelve Months (TTM) data; "
            "falls back to Annual if TTM unavailable."
        )
    },
}


class ContextBuilder:
    def __init__(self, dashboard_data: DashboardData, fx_engine: FXEngine) -> None:
        self.dashboard_data = dashboard_data
        self.fx_engine = fx_engine

    def _sanitize(self, data: pl.DataFrame) -> pl.DataFrame:
        """Sanitize DataFrame for LLM context export."""
        float_cols = [col for col, dtype in data.schema.items() if dtype == pl.Float64]
        date_cols = [col for col, dtype in data.schema.items() if dtype == pl.Date]

        exprs = []
        for col in date_cols:
            exprs.append(pl.col(col).dt.strftime("%Y-%m-%d").fill_null("N/A").alias(col))

        for col in float_cols:
            exprs.append(pl.col(col).round(2).fill_null("N/A").alias(col))
        if exprs:
            data = data.with_columns(exprs)
        return data

    def _to_split_json(self, data: pl.DataFrame) -> dict[str, Any]:
        """Convert DataFrame to a JSON structure suitable for LLM context."""
        return {
            "columns": data.columns,
            "data": data.rows(),
        }

    def get_metadata(self, tickers: list[str]) -> dict[str, Any]:
        df_metadata = (
            self.dashboard_data.metadata.filter(pl.col("ticker").is_in(tickers))
            .select("ticker", "name", "sector", "country", "industry")
            .pipe(self._sanitize)
            .pipe(self._to_split_json)
        )
        return df_metadata

    def get_price_history(self, tickers: list[str]) -> dict[str, Any]:
        raw_prices = self.dashboard_data.prices.filter(pl.col("ticker").is_in(tickers)).select(
            "ticker", "date", "low", "close", "volume"
        )

        last_30_day_prices = raw_prices.sort(["ticker", "date"]).group_by("ticker").tail(30)

        every_28_day_prices = (
            raw_prices.sort(["ticker", "date"])
            .with_columns(
                # dummy index
                (pl.arange(0, pl.count()).over("ticker") % 28).alias("day_mod_28")
            )
            .filter(pl.col("day_mod_28") == 0)
            .drop("day_mod_28")
        )

        export_prices = (
            pl.concat([last_30_day_prices, every_28_day_prices])
            .unique()
            .sort(["ticker", "date"])
            .pipe(self._sanitize)
            .pipe(self._to_split_json)
        )
        return export_prices

    def get_valuations(self, tickers: list[str]) -> dict[str, Any]:
        export_valuations = (
            (
                self.dashboard_data.prices.filter(pl.col("ticker").is_in(tickers))
                .group_by("ticker")
                .last()
            )
            .pipe(
                self.fx_engine.convert_multiple_to_target,
                amount_cols=["close", "fair_value"],
                source_currency_col="currency",
            )
            .select(
                "ticker",
                "date",
                "close",
                "close_EUR",
                "fair_value",
                "fair_value_EUR",
                "pe_ratio",
                "ps_ratio",
                "fcf_yield",
                "dividend_yield",
                "forward_pe",
                "median_pe",
                "peg_ratio",
            )
            .pipe(self._sanitize)
            .pipe(self._to_split_json)
        )
        export_valuations["metric_descriptions"] = METRIC_DESCRIPTIONS.get("valuations", {})
        export_valuations["source_descriptions"] = METRIC_DESCRIPTIONS.get("sources", {})
        return export_valuations

    def get_fundamentals(self, tickers: list[str]) -> dict[str, Any]:
        export_fundamentals = (
            self.dashboard_data.fundamentals.filter(
                pl.col("ticker").is_in(tickers) & (pl.col("period_type") == "annual")
            )
            .sort(["ticker", "report_date"])
            .group_by("ticker")
            .last()
            .select(
                "ticker",
                "report_date",
                "currency",
                "basic_eps",
                "diluted_eps",
                "roce",
                "rotce",
                "net_debt_to_ebit",
                "net_debt_to_ebitda",
                "net_profit_margin",
                "gross_margin",
                "ebit_margin",
                "cash_conversion_ratio",
                "revenue_growth",
                "net_income_growth",
            )
            .pipe(self._sanitize)
            .pipe(self._to_split_json)
        )
        export_fundamentals["metric_descriptions"] = METRIC_DESCRIPTIONS.get("fundamentals", {})
        export_fundamentals["source_descriptions"] = METRIC_DESCRIPTIONS.get("sources", {})
        return export_fundamentals

    def get_timing(self, tickers: list[str]) -> dict[str, Any]:
        export_timing = (
            calculate_volatility_metrics(
                df_prices=self.dashboard_data.prices,
                window_days=250,
                selected_tickers=tickers,
            )
            .group_by("ticker")
            .last()
            .pipe(
                self.fx_engine.convert_multiple_to_target,
                amount_cols=["close", "sma_200", "sma_50"],
                source_currency_col="currency",
            )
            .select(
                "ticker",
                "date",
                "close",
                "close_EUR",
                "sma_50",
                "sma_50_EUR",
                "std_50",
                "sma_200",
                "sma_200_EUR",
                "std_200",
                "z_score",
                "dist_200_pct",
                "vola_annual_pct",
            )
            .pipe(self._sanitize)
            .pipe(self._to_split_json)
        )
        export_timing["metric_descriptions"] = METRIC_DESCRIPTIONS.get("timing", {})
        return export_timing

    def get_strategy_context(self) -> dict[str, Any]:
        """
        Returns the distilled investment strategy to frame the LLM's analysis.
        Derived from 'Depot Strategie V2' & 'The Quality Core'.
        """
        return {
            "philosophy": {
                "style": "Global Quality Investing (Long-term Buy & Hold)",
                "core_values": [
                    "Quality at a Fair Price",
                    (
                        "Impact > Purity (Support 'Best-in-Class' transformers "
                        "over niche green tech)"
                    ),
                    "Owner-Operator Governance (No Black Boxes)",
                ],
                "portfolio_structure": (
                    "Concentrated Quality Core (15-20 stocks) + "
                    "Developed Markets ETF Foundation."
                ),
                "goal": (
                    "Reduce ETF dependency over time; " "wealth accumulation through substance."
                ),
            },
            "selection_criteria": {
                "moat": "Required. Must have a credible 10-Year Scenario (No Melting Ice Cubes).",
                "geography": "Europe Core (Values/Hidden Champs) + Global Select (US Monopolies).",
                "analysis_model": "4-Factor Model (Earnings Drivers) > GICS Sectors.",
            },
            "execution_rules": {
                "gardener_rule": (
                    "Max 15% per single stock. " "Rebalance organically (fresh cash/dividends)."
                ),
                "sleep_test": (
                    "Volatility is acceptable; ",
                    "sell ONLY on thesis breach (Governance fraud, Moat erosion).",
                ),
                "entry_strategy": (
                    "Fair Value = Lump Sum. ",
                    "Expensive/Unsure = Savings Plan/Watchlist.",
                ),
                "cash_rule": r"Invested > Timing. Max 5-10% dry powder.",
            },
        }
