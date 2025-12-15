"""Stock detail logic package.

Exports main data loading functions for stock-specific analysis.
"""

import polars as pl

from src.app.logic.stock_detail.loader import (
    get_available_tickers,
    get_ticker_data,
    get_ticker_fundamentals,
)

__all__ = [
    "get_ticker_data",
    "get_ticker_fundamentals",
    "get_available_tickers",
]


def filter_data_by_date_range(
    df_prices: pl.DataFrame,
    df_fund: pl.DataFrame,
    start_date: str,
    end_date: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Filter DataFrame to a specific date range.

    Args:
        df: Input DataFrame with a 'date' column
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format

    Returns:
        Filtered DataFrame within the specified date range
    """
    df_prices_filtered = df_prices.filter(
        (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
    )
    if not df_fund.is_empty():
        df_fund_filtered = df_fund.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )
    else:
        df_fund_filtered = df_fund
    return (df_prices_filtered, df_fund_filtered)
