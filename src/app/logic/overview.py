"""Logic layer for portfolio overview page.

Handles portfolio performance calculations and KPI aggregation.
"""

import polars as pl

from src.analysis.fx import FXEngine
from src.app.logic.data_loader import DashboardData


def get_market_snapshot(
    data: DashboardData,
    fx_engine: FXEngine,
    tickers: list[str] | None = None,
) -> pl.DataFrame:
    if tickers is not None:
        df_prices = data.prices.filter(pl.col("ticker").is_in(tickers))
        df_fundamentals = data.fundamentals.filter(pl.col("ticker").is_in(tickers))

    df_prices_currency = fx_engine.convert_to_target(
        df_prices, "adj_close", source_currency_col="currency"
    )
    latest_prices = (
        df_prices_currency.sort("date")
        .group_by("ticker")
        .last()
        .rename({"close": "latest_price", "date": "price_date"})
        .with_columns(
            # market cap in billion euros
            (pl.col("adj_close_EUR") * pl.col("diluted_average_shares") / 1_000_000_000).alias(
                "market_cap_b_eur"
            )
        )
    )

    duplicate_columns = [
        col for col in df_fundamentals.columns if col in latest_prices.columns and col != "ticker"
    ]

    latest_fundamentals = (
        df_fundamentals.sort("date")
        .group_by("ticker")
        .last()
        .rename({"date": "fundamentals_date"})
        .drop(duplicate_columns)
    )

    percentage_cols = [
        "fcf_yield",
        "roce",
        "gross_margin",
        "ebit_margin",
        "revenue_growth",
    ]
    percent_transforms = [(pl.col(col) * 100).alias(col) for col in percentage_cols]

    snapshot = (
        latest_prices.join(latest_fundamentals, on="ticker", how="left")
        .select(
            [
                "ticker",
                "data_lag_days",
                "valuation_source",
                "latest_price",
                "market_cap_b_eur",
                "pe_ratio",
                "fcf_yield",
                "roce",
                "gross_margin",
                "ebit_margin",
                "revenue_growth",
                "net_debt_to_ebit",
            ]
        )
        .with_columns(percent_transforms)
    ).join(data.metadata, on="ticker", how="left")

    return snapshot
