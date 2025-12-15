"""Data loading logic for stock detail page.

Filters and prepares ticker-specific data for visualization.
"""

import polars as pl
from loguru import logger

from src.config.settings import load_config

config = load_config()


def get_ticker_data(
    ticker: str,
    df_prices: pl.DataFrame,
) -> pl.DataFrame:
    """Extract price data for a specific ticker.

    Args:
        ticker: Stock ticker symbol
        df_prices: Full price dataset with valuation metrics

    Returns:
        DataFrame filtered to ticker with columns:
            date, open, high, low, close, volume, fcf_yield, dividend_yield, etc.
    """
    df_ticker = df_prices.filter(pl.col("ticker") == ticker).sort("date")

    if df_ticker.is_empty():
        logger.warning(f"No price data found for ticker '{ticker}'")
    else:
        logger.info(f"Loaded {df_ticker.height} price records for {ticker}")

    return df_ticker


def get_ticker_fundamentals(
    ticker: str,
    df_fund: pl.DataFrame,
) -> pl.DataFrame:
    """Extract fundamental data for a specific ticker.

    Args:
        ticker: Stock ticker symbol
        df_fund: Full fundamentals dataset with calculated metrics

    Returns:
        DataFrame filtered to ticker with columns:
            date, roce, free_cash_flow, net_debt, margins, etc.
    """
    df_ticker = df_fund.filter(pl.col("ticker") == ticker).sort("date")

    if df_ticker.is_empty():
        logger.warning(f"No fundamental data found for ticker '{ticker}'")
    else:
        logger.info(f"Loaded {df_ticker.height} fundamental records for {ticker}")

    return df_ticker


def get_available_tickers(portfolio_name: str | None = None) -> list[str]:
    """Get sorted list of all available tickers.

    Args:
        portfolio_name: Optional portfolio name to filter tickers

    Returns:
        Sorted list of unique ticker symbols
    """
    if portfolio_name:
        if config.portfolios is None:
            return []
        portfolio = config.portfolios.portfolios.get(portfolio_name)
        tickers = portfolio.tickers if portfolio else []
    else:
        if config.portfolios is None:
            return []
        tickers = list(config.portfolios.all_tickers)
    logger.debug(f"Found {len(tickers)} unique tickers in dataset")
    return tickers
