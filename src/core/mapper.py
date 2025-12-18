"""
ETL Mapping Layer: Transforms external data provider formats to domain models.

This module bridges the gap between raw data (yfinance pandas DataFrames)
and our internal domain representation (Polars DataFrames + Pydantic models).
"""

from datetime import datetime

import pandas as pd
import polars as pl
from loguru import logger

from src.core.domain_models import STOCK_PRICE_SCHEMA, FinancialReport, ReportType


def map_prices_to_df(pdf: pd.DataFrame, ticker: str, currency: str) -> pl.DataFrame:
    """
    Convert yfinance price history to Polars DataFrame with strict schema.

    Args:
        pdf: Raw pandas DataFrame from yfinance.history()
        ticker: Stock ticker symbol

    Returns:
        Polars DataFrame matching STOCK_PRICE_SCHEMA
    """
    # Reset index (yfinance typically uses Date as index)
    pdf_reset = pdf.reset_index()

    # remove multi-index and lowercase columns
    pdf_reset.columns = [col[0].lower() for col in pdf_reset.columns]

    # Convert to Polars immediately
    prices = pl.from_pandas(pdf_reset)

    # Add ticker column
    prices = prices.with_columns(
        pl.lit(ticker).alias("ticker"),
        pl.lit(currency).alias("currency"),
        pl.col("date").cast(pl.Date),
    )

    if "dividends" in prices.columns:
        prices = prices.rename({"dividends": "dividend"})
    else:
        prices = prices.with_columns(pl.lit(0.0).alias("dividend"))
    prices = prices.with_columns(pl.col("dividend").fill_null(0.0))

    # Ensure Date column exists (might be named differently)
    if "date" not in prices.columns and "index" in prices.columns:
        prices = prices.rename({"index": "date"})

    # Handle adj_close column name variation
    if "adj close" in prices.columns and "adj_close" not in prices.columns:
        prices = prices.rename({"adj close": "adj_close"})
    if "adj_close" not in prices.columns:
        # If adj_close is missing, close is already adjusted itself hence copy close to adj_close
        prices = prices.with_columns(pl.col("close").alias("adj_close"))

    # Select required columns and cast to STOCK_PRICE_SCHEMA
    prices = prices.select([pl.col(name).cast(dtype) for name, dtype in STOCK_PRICE_SCHEMA.items()])

    logger.debug(f"Mapped {len(prices)} price rows for {ticker}")
    return prices


def map_fundamentals_to_domain(
    pdf: pd.DataFrame,
    ticker: str,
    report_type: ReportType,
    currency: str,
) -> list[FinancialReport]:
    """
    Transform yfinance financial statements to domain models.

    yfinance returns transposed data (metrics as rows, dates as columns).
    We transpose it to have dates as rows for iteration.

    Args:
        pdf: Raw pandas DataFrame from yfinance (quarterly_financials, etc.)
        ticker: Stock ticker symbol
        report_type: ANNUAL or QUARTERLY

    Returns:
        List of FinancialReport domain objects
    """
    if pdf.empty:
        logger.warning(f"Empty financial data for {ticker} ({report_type})")
        return []

    # Transpose: dates become rows, metrics become columns
    pdf_transposed = pdf.T

    # Normalize column names to lowercase and strip whitespace
    pdf_transposed.columns = pdf_transposed.columns.str.lower().str.strip()

    reports = []

    for report_date, row in pdf_transposed.iterrows():
        try:
            # Parse date (yfinance returns datetime-like index)
            if isinstance(report_date, pd.Timestamp):
                parsed_date = report_date.date()
            elif isinstance(report_date, datetime):
                parsed_date = report_date.date()
            else:
                parsed_date = pd.to_datetime(report_date).date()

            # Map yfinance keys to domain model fields
            # Note: yfinance column names can vary slightly
            report = FinancialReport(
                ticker=ticker,
                report_date=parsed_date,
                period_type=report_type,
                currency=currency,
                # Income Statement
                revenue=_safe_float(row, ["total revenue", "revenue", "operating revenue"]),
                gross_profit=_safe_float(row, ["gross profit", "gross income"]),
                ebit=_safe_float(
                    row,
                    [
                        "ebit",
                        "earnings before interest and tax",
                        "operating income",
                        "operating profit",
                    ],
                ),
                net_income=_safe_float(
                    row,
                    [
                        "net income",
                        "net income common stockholders",
                        "net income from continuing operations",
                    ],
                ),
                tax_provision=_safe_float(
                    row,
                    [
                        "tax provision",
                        "tax expense",
                        "income tax expense",
                        "provision for income taxes",
                    ],
                ),
                diluted_eps=_safe_float(
                    row,
                    [
                        "diluted eps",
                        "diluted earnings per share",
                        "earnings per share diluted",
                    ],
                ),
                basic_eps=_safe_float(row, ["basic eps", "basic earnings per share"]),
                # Cash Flow
                operating_cash_flow=_safe_float(
                    row, ["operating cash flow", "total cash from operating activities"]
                ),
                capital_expenditure=_safe_float(
                    row,
                    [
                        "capital expenditure",
                        "capital expenditures",
                        "purchase of property, plant and equipment",
                        "purchase of ppe",
                    ],
                ),
                free_cash_flow=_safe_float(row, ["free cash flow"]),
                # Shares
                basic_average_shares=_safe_float(
                    row, ["basic average shares", "ordinary shares number"]
                ),
                diluted_average_shares=_safe_float(
                    row, ["diluted average shares", "ordinary shares number"]
                ),
                # Balance Sheet
                total_assets=_safe_float(row, ["total assets"]),
                total_current_liabilities=_safe_float(
                    row, ["total current liabilities", "current liabilities"]
                ),
                total_equity=_safe_float(
                    row,
                    [
                        "total equity",
                        "stockholders equity",
                        "total stockholder equity",
                        "total equity and gross minority interest",
                    ],
                ),
                long_term_debt=_safe_float(
                    row,
                    [
                        "long term debt",
                        "long-term debt",
                        "long term debt and capital lease obligations",
                    ],
                ),
                cash_and_equivalents=_safe_float(row, ["cash and cash equivalents", "cash"]),
            )

            reports.append(report)
            logger.debug(f"Mapped {report_type} report for {ticker} on {parsed_date}")

        except Exception as e:
            logger.error(f"Failed to map report for {ticker} at {report_date}: {e}")
            continue

    logger.info(f"Mapped {len(reports)} {report_type} reports for {ticker}")
    return reports


def _safe_float(row: pd.Series, keys: list[str]) -> float | None:
    """
    Extract numeric value from pandas Series using multiple possible keys.
    Due to variations in yfinance column names, we try several options.
    The matching is case-insensitive and ignores leading/trailing whitespace.

    Args:
        row: pandas Series (one row from transposed DataFrame)
        keys: List of possible column names to try (case-insensitive)

    Returns:
        Float value if found and valid, None otherwise
    """
    # Create lowercase index for case-insensitive lookup
    lookup_map = {idx.strip().lower(): idx for idx in row.index if isinstance(idx, str)}

    for key in keys:
        key_clean = key.strip().lower()
        if key_clean in lookup_map:
            original_key = lookup_map[key_clean]
            value = row[original_key]
            if pd.notna(value):
                try:
                    if isinstance(value, str):
                        return float(value.replace(",", ""))
                    return float(value)
                except (ValueError, TypeError):
                    continue
    return None
