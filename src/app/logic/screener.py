import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.app.logic.data_loader import DashboardData, GlobalDataLoader
from src.app.views.common import (
    render_empty_state,
)


def prepare_screener_snapshot(
    df_prices: pl.DataFrame,
    df_fundamentals: pl.DataFrame,
    df_metadata: pl.DataFrame,
    fx_engine: FXEngine,
    selected_tickers: list[str],
) -> pl.DataFrame:
    """Prepare a snapshot DataFrame for the stock screener."""
    if not selected_tickers:
        return pl.DataFrame()

    if "info" not in df_metadata.columns:
        df_metadata = df_metadata.with_columns(
            (pl.col("country") + " " + pl.col("sector")).alias("info")
        )

    df_fundamentals_latest = (
        df_fundamentals.sort(["ticker", "report_date"])
        .group_by("ticker")
        .agg(
            pl.last("roce").alias("roce"),
            pl.last("ebit_margin").alias("ebit_margin"),
            pl.last("net_debt_to_ebit").alias("net_debt_to_ebit"),
            pl.last("revenue_growth").alias("revenue_growth"),
        )
    )

    df_prices_latest = (
        df_prices.filter(pl.col("ticker").is_in(selected_tickers))
        .pipe(
            lambda df: fx_engine.convert_multiple_to_target(
                df, amount_cols=["close", "fair_value"], source_currency_col="currency"
            )
        )
        .sort(["ticker", "date"])
        .with_columns(
            (pl.col("pe_ratio").rank("average") / pl.col("pe_ratio").count())
            .over("ticker")
            .alias("pe_rank")
        )
        .group_by("ticker")
        .agg(
            pl.last("close_EUR").alias("close"),
            pl.last("fair_value_EUR").alias("fair_value"),
            pl.last("dividend_yield").alias("dividend_yield"),
            pl.last("fcf_yield").alias("fcf_yield"),
            pl.last("currency").alias("currency"),
            pl.last("pe_ratio").alias("pe_ratio"),
            pl.last("pe_rank").alias("pe_rank"),
            pl.last("data_lag_days").alias("data_lag_days"),
            # pe percentiles
            pl.median("pe_ratio").alias("pe_ratio_median"),
            pl.col("pe_ratio").quantile(0.25).alias("pe_ratio_p25"),
            pl.col("pe_ratio").quantile(0.4).alias("pe_ratio_p40"),
            pl.col("pe_ratio").quantile(0.6).alias("pe_ratio_p60"),
            pl.col("pe_ratio").quantile(0.75).alias("pe_ratio_p75"),
            # take last 30 days of closes and put to a list
            pl.tail("close_EUR", 30).alias("close_30d"),
        )
        .join(
            df_fundamentals_latest,
            on="ticker",
            how="left",
        )
        .with_columns(
            # upside
            ((pl.col("fair_value") / pl.col("close")) - 1.0).alias("upside"),
            # PEG ratio
            pl.when(pl.col("revenue_growth") > 0.05)
            .then(pl.col("pe_ratio") / (100 * pl.col("revenue_growth")))
            .otherwise(None)
            .alias("peg_ratio"),
        )
        .join(
            df_metadata.select(["ticker", "name", "info"]),
            on="ticker",
            how="left",
        )
    )
    return df_prices_latest


# we need the caching to stabilize the selection
@st.cache_data(ttl=3600, show_spinner="Loading data...")  # type: ignore[misc]
def load_all_stock_data() -> tuple[DashboardData, dict[str | None, list[str]]]:
    # Load data
    try:
        loader = GlobalDataLoader()
        dashboard_data = loader.load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        logger.error(f"Data loading error: {e}", exc_info=True)
        raise e
    # Get available tickers
    if dashboard_data.prices.is_empty():
        render_empty_state("No price data available")
        st.stop()

    portfolio_dict_raw = loader.config.portfolios.portfolios if loader.config.portfolios else {}
    portfolio_dict = {p.display_name: p.tickers for p in portfolio_dict_raw.values()}

    return dashboard_data, portfolio_dict
