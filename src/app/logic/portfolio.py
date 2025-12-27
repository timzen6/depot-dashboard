from dataclasses import dataclass
from datetime import timedelta

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

    df_history_raw = portfolio_engine.calculate_portfolio_history(portfolio, df_prices, fx_engine)

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
    q = df_history_target_currency.select("position_value_EUR", "date", "ticker").with_columns(
        pl.col("date").dt.offset_by("1y").alias("matching_date")
    )

    df_history_target_currency = (
        df_history_target_currency.join_asof(
            q,
            left_on="date",
            right_on="matching_date",
            by="ticker",
            strategy="backward",
            suffix="_yoy",
        )
        .with_columns(
            # yoy return
            (
                (pl.col("position_value_EUR") - pl.col("position_value_EUR_yoy"))
                / pl.col("position_value_EUR_yoy")
                * 100
            ).alias("yoy_return_pct"),
            # yoy total return including dividends
            (
                (
                    (
                        pl.col("position_value_EUR")
                        + pl.col("position_dividend_yoy_EUR").fill_null(0)
                    )
                    - pl.col("position_value_EUR_yoy")
                )
                / pl.col("position_value_EUR_yoy")
                * 100
            ).alias("yoy_total_return_pct"),
        )
        .drop("matching_date", "position_value_EUR_yoy")
    )

    return df_history_target_currency


def filter_days_with_incomplete_tickers(df_history: pl.DataFrame) -> pl.DataFrame:
    """Filter out dates where not all tickers have price data.

    Prevents artificial portfolio value drops caused by missing data on holidays.
    Different exchanges may have different trading calendars.

    Args:
        df_history: Portfolio history with ticker-level data

    Returns:
        Filtered DataFrame containing only dates with complete ticker coverage
    """
    required_tickers = df_history.select(pl.col("ticker").unique()).height
    dates_with_all_tickers = (
        df_history.group_by("date")
        .agg(pl.count("ticker").alias("ticker_count"))
        .filter(pl.col("ticker_count") == required_tickers)
        .select("date")
    )
    return df_history.join(dates_with_all_tickers, on="date", how="inner")


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
        df_history.pipe(filter_days_with_incomplete_tickers)
        .group_by("date")
        .agg(
            pl.sum("position_value_EUR").alias("total_value"),
            pl.sum("position_dividend_yoy_EUR").alias("total_dividend_yoy_EUR"),
        )
        .sort("date")
    )

    # Current and start values
    current_value = df_daily.select(pl.last("total_value")).item()
    if "total_dividend_yoy_EUR" in df_daily.columns:
        current_yoy_dividend_value = df_daily.select(pl.last("total_dividend_yoy_EUR")).item()
    else:
        current_yoy_dividend_value = 0
    start_value = df_daily.select(pl.first("total_value")).item()
    start_date = df_daily.select(pl.first("date")).item()
    latest_date = df_daily.select(pl.last("date")).item()

    # Total return this is not entirely correct if there are dividends
    total_return_pct = ((current_value - start_value) / start_value * 100) if start_value else 0.0

    # Year-over-year return (last 365 days)
    one_year_ago = latest_date - timedelta(days=365) or latest_date

    df_yoy = df_daily.filter(pl.col("date") >= one_year_ago).sort("date")
    if df_yoy.height > 0:
        yoy_start = df_yoy.select(pl.first("total_value")).item()
        yoy_return_pct = (
            ((current_value - yoy_start + current_yoy_dividend_value) / yoy_start * 100)
            if yoy_start
            else 0.0
        )
    else:
        yoy_return_pct = total_return_pct

    return PortfolioKPIs(
        current_value=float(current_value),
        current_yoy_dividend_value=float(current_yoy_dividend_value),
        start_value=float(start_value),
        total_return_pct=float(total_return_pct),
        yoy_return_pct=float(yoy_return_pct),
        start_date=str(start_date),
        latest_date=str(latest_date),
    )
