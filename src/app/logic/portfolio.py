from dataclasses import dataclass

import polars as pl
from loguru import logger

from src.analysis.fx import FXEngine
from src.analysis.portfolio import PortfolioEngine
from src.config.models import Portfolio


def get_portfolio_performance(
    portfolio: Portfolio,
    df_prices: pl.DataFrame,
    fx_engine: FXEngine,
    portfolio_engine: PortfolioEngine,
) -> pl.DataFrame:
    """Calculate historical performance for a specific portfolio."""

    logger.info(f"Calculating performance for portfolio '{portfolio.name}'")

    df_history_raw = portfolio_engine.calculate_portfolio_history(
        portfolio,
        df_prices.pipe(
            lambda df: fx_engine.convert_to_target(
                df, amount_col="close", source_currency_col="currency"
            )
        ),
    )

    df_history_target_currency = fx_engine.convert_to_target(
        df_history_raw,
        amount_col="position_value",
        source_currency_col="currency",
    )
    if "position_dividend_yoy" in df_history_target_currency.columns:
        df_history_target_currency = fx_engine.convert_to_target(
            df_history_target_currency,
            amount_col="position_dividend_yoy",
            source_currency_col="currency",
        )
    if "position_dividend" in df_history_target_currency.columns:
        df_history_target_currency = fx_engine.convert_to_target(
            df_history_target_currency,
            amount_col="position_dividend",
            source_currency_col="currency",
        )
    else:
        df_history_target_currency = df_history_target_currency.with_columns(
            pl.lit(0.0).alias("position_dividend_EUR"),
        )

    if "cashflow_EUR" not in df_history_target_currency.columns:
        df_history_target_currency = df_history_target_currency.with_columns(
            pl.lit(0.0).alias("cashflow_EUR"),
        )

    df_history_target_currency = (
        df_history_target_currency.sort(["ticker", "date"])
        .with_columns(
            # calculate daily return close_today - close_yesterday - cashflow
            (
                pl.col("position_value_EUR")
                - pl.col("position_value_EUR").shift(1).fill_null(0)
                - pl.col("cashflow_EUR").fill_null(0)
                + pl.col("position_dividend_EUR").fill_null(0)
            )
            .over("ticker")
            .fill_null(0)
            .alias("daily_absolute_pnl_EUR"),
        )
        .sort(["ticker", "date"])
        .with_columns(
            pl.col("position_dividend_EUR")
            .rolling_sum_by("date", window_size="1y")
            .over("ticker")
            .alias("position_dividend_yoy_EUR"),
            pl.col("daily_absolute_pnl_EUR")
            .rolling_sum_by("date", window_size="1y")
            .over("ticker")
            .alias("yoy_absolute_pnl_EUR"),
            pl.col("daily_absolute_pnl_EUR")
            .cum_sum()
            .over("ticker")
            .alias("total_absolute_pnl_EUR"),
        )
        .with_columns(
            # calculate yoy pct
            (
                (
                    pl.col("yoy_absolute_pnl_EUR")
                    / (pl.col("position_value_EUR") - pl.col("yoy_absolute_pnl_EUR"))
                )
                * 100
            ).alias("yoy_return_pct")
        )
    )

    return df_history_target_currency


def fill_days_with_missing_tickers(df_history: pl.DataFrame) -> pl.DataFrame:
    """Align all tickers to the global trading calendar, forward-filling gaps up to the max date.

    Uses a cross-join master grid so tickers missing recent days are extended
    to the global max date rather than left truncated.
    """
    global_calendar = df_history.select("date").unique()
    tickers = df_history.select("ticker").unique()

    master_grid = tickers.join(global_calendar, how="cross").sort(["ticker", "date"])

    return master_grid.join_asof(
        df_history.sort(["ticker", "date"]),
        on="date",
        by="ticker",
        strategy="backward",
    ).drop_nulls(subset=["position_value_EUR"])


@dataclass
class PortfolioKPIs:
    current_value: float
    current_yoy_dividend_value: float
    start_value: float
    total_return_pct: float
    yoy_return_pct: float
    start_date: str
    latest_date: str


def get_portfolio_kpis(df_history: pl.DataFrame) -> PortfolioKPIs:
    """Calculate key performance indicators from portfolio history.

    Args:
        df_history: Portfolio history with total_value column

    Returns:
        Dictionary with KPIs:
            - current_value: Latest total value
            - start_value: First total value
            - total_return_pct: Percentage return since inception
            - yoy_return_pct: Year-over-year return
            - latest_date: Most recent date in history
    """
    if df_history.is_empty():
        logger.warning("Portfolio history is empty, returning zero KPIs")
        return PortfolioKPIs(
            current_value=0.0,
            current_yoy_dividend_value=0.0,
            start_value=0.0,
            total_return_pct=0.0,
            yoy_return_pct=0.0,
            start_date="N/A",
            latest_date="N/A",
        )

    df_daily = (
        df_history.pipe(fill_days_with_missing_tickers)
        .group_by("date")
        .agg(
            pl.sum("position_value_EUR").alias("total_value"),
            pl.sum("position_dividend_yoy_EUR").alias("total_dividend_yoy_EUR"),
            pl.sum("total_absolute_pnl_EUR").alias("total_absolute_pnl_EUR"),
            pl.sum("yoy_absolute_pnl_EUR").alias("yoy_absolute_pnl_EUR"),
        )
        .sort("date")
    )

    # Current and start values
    current_value = df_daily.select(pl.last("total_value")).item()
    if "total_dividend_yoy_EUR" in df_daily.columns:
        current_yoy_dividend_value = df_daily.select(pl.last("total_dividend_yoy_EUR")).item()
    else:
        current_yoy_dividend_value = 0

    total_pnl = df_daily.select(pl.last("total_absolute_pnl_EUR")).item()

    if abs(total_pnl - current_value) < 1e-6:
        logger.warning(
            "Total PnL is very close to current value, "
            "setting total return to 0% to avoid division by zero"
        )
        total_return_pct = 0.0
    else:
        total_return_pct = (total_pnl / (current_value - total_pnl) * 100) if current_value else 0.0

    yoy_return = df_daily.select(pl.last("yoy_absolute_pnl_EUR")).item()
    yoy_return_pct = (yoy_return / (current_value - yoy_return) * 100) if current_value else 0.0

    start_date = df_daily.select(pl.first("date")).item()
    latest_date = df_daily.select(pl.last("date")).item()
    start_value = df_daily.select(pl.first("total_value")).item()

    return PortfolioKPIs(
        current_value=float(current_value),
        current_yoy_dividend_value=float(current_yoy_dividend_value),
        start_value=float(start_value),
        total_return_pct=float(total_return_pct),
        yoy_return_pct=float(yoy_return_pct),
        start_date=str(start_date),
        latest_date=str(latest_date),
    )
