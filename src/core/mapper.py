"""
ETL Mapping Layer: Transforms external data provider formats to domain models.

This module bridges the gap between raw data (yfinance pandas DataFrames)
and our internal domain representation (Polars DataFrames + Pydantic models).
"""

from datetime import date, datetime
from typing import Any

import pandas as pd
import polars as pl
from loguru import logger

from src.core.domain_models import (
    STOCK_PRICE_SCHEMA,
    AssetMetadata,
    AssetType,
    FinancialReport,
    ReportType,
    Sector,
)


def map_sector(sector_str: str | None) -> Sector | None:
    """
    Map yfinance sector string to Sector enum.

    Args:
        sector_str: yfinance sector string (e.g. "Technology", "Healthcare")

    Returns:
        Corresponding Sector enum value or None if unrecognized
    """
    if not sector_str:
        return None

    sector_synonyms = {
        "Technology": ["Tech"],
        "Healthcare": ["Health Care"],
        "Financials": ["Finance", "Financial Services"],
        "Consumer Discretionary": [
            "Consumer Services",
            "Consumer Cyclical",
            "Discretionary",
        ],
        "Communication": [
            "Communication Services",
            "Telecommunication",
            "Telecom",
            "Communications",
        ],
        "Industrials": ["Industrial Goods"],
        "Consumer Staples": ["Staples", "Consumer Defensive"],
        "Energy": ["Oil & Gas"],
        "Utilities": ["Utilities"],
        "Real Estate": ["Property"],
        "Materials": ["Basic Materials"],
    }

    tmp_str = sector_str.lower().strip()
    for sector, synonyms in sector_synonyms.items():
        if tmp_str == sector.lower().strip() or tmp_str in [s.lower().strip() for s in synonyms]:
            return Sector(sector)

    logger.warning(f"Unrecognized sector '{sector_str}'")
    return None


def map_asset_type(info: dict[str, str]) -> AssetType:
    """
    Map yfinance quoteType string to AssetType enum.

    Args:
        quote_type: yfinance quoteType string (e.g. "EQUITY", "ETF", "CURRENCY")

    Returns:
        Corresponding AssetType enum value
    """
    quote_type_raw = info.get("quoteType", None)
    if not quote_type_raw:
        logger.warning(
            f"Missing quoteType in ticker info for {info.get('symbol', 'unknown')}. "
            "Defaulting to STOCK."
        )
        return AssetType.STOCK

    direct_type_mapping = {
        "etf": AssetType.ETF,
        "currency": AssetType.FX,
        "index": AssetType.FX,
        "cryptocurrency": AssetType.CRYPTO,
        "future": AssetType.COMMODITY,
        "mutualfund": AssetType.ETF,
    }

    quote_type = quote_type_raw.lower()
    if not quote_type:
        return AssetType.STOCK
    simple_type = direct_type_mapping.get(quote_type, None)
    if simple_type is not None:
        return simple_type

    if quote_type == "equity":
        fund_family = _safe_str(info, ["fundFamily"])

        if fund_family:
            fund_lower = fund_family.lower()
            if "etf" in fund_lower or "exchange traded" in fund_lower:
                return AssetType.ETF
            # We might check for commodity funds here as well in future
        return AssetType.STOCK
    logger.warning(
        f"Unrecognized quoteType '{quote_type}' for "
        f"{info.get('symbol', 'unknown')}. Defaulting to STOCK."
    )
    return AssetType.STOCK


def map_ticker_info_to_asset_metadata(
    info: dict[str, str], calendar: dict[str, Any] | None = None
) -> AssetMetadata:
    """
    Map yfinance ticker info dictionary to AssetMetadata domain model.

    Args:
        info: Dictionary from yfinance.Ticker.info

    Returns:
        AssetMetadata domain object
    """
    asset_type = map_asset_type(info)
    sector = map_sector(_safe_str(info, ["sector", "sectorDisp", "sectorKey"]))
    short_name = _safe_str(info, ["shortName", "displayName"])
    if short_name:
        short_name = short_name.replace("   I", "").strip()
    name = _safe_str(info, ["longName", "shortName", "displayName", "name", "ticker"])
    if calendar is not None:
        dividend_date = _safe_date(calendar, ["Dividend Date", "dividendDate"])
        earnings_date = _safe_date(calendar, ["Earnings Date", "earningsDate"])
    else:
        dividend_date = None
        earnings_date = None
    if name:
        name = name.replace("   I", "").strip()
    else:
        name = "Unknown"
    asset_metadata = AssetMetadata(
        ticker=_safe_str(info, ["symbol", "ticker"]) or "UNKNOWN",
        name=name,
        asset_type=asset_type,
        short_name=short_name,
        exchange=_safe_str(info, ["exchange", "exchangeName"]),
        currency=_safe_str(info, ["currency"]) or "USD",
        country=_safe_str(info, ["country", "countryOfIncorporation"]),
        sector_raw=_safe_str(info, ["sector", "sectorDisp", "sectorKey"]),
        sector=sector,
        industry=_safe_str(info, ["industry", "industryDisp", "industryKey"]),
        # important metrics for quick analysis, we cannot get them in the fundamentals
        forward_pe=_get_float(info, ["forwardPE"]),
        forward_eps=_get_float(info, ["forwardEps"]),
        display_name=_safe_str(info, ["displayName"]),
        dividend_date=dividend_date,
        earnings_date=earnings_date,
    )
    logger.debug(f"Mapped AssetMetadata for {asset_metadata.name} ({asset_metadata.exchange})")
    return asset_metadata


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

            if parsed_date > datetime.now().date():
                logger.warning(f"Skipping future report date {parsed_date} for {ticker}")
                continue

            row_data = row.to_dict()

            # Map yfinance keys to domain model fields
            # Note: yfinance column names can vary slightly
            report = FinancialReport(
                ticker=ticker,
                report_date=parsed_date,
                period_type=report_type,
                currency=currency,
                # Income Statement
                revenue=_get_float(row_data, ["total revenue", "revenue", "operating revenue"]),
                gross_profit=_get_float(row_data, ["gross profit", "gross income"]),
                ebit=_get_float(
                    row_data,
                    [
                        "ebit",
                        "earnings before interest and tax",
                        "operating income",
                        "operating profit",
                    ],
                ),
                net_income=_get_float(
                    row_data,
                    [
                        "net income",
                        "net income common stockholders",
                        "net income from continuing operations",
                    ],
                ),
                tax_provision=_get_float(
                    row_data,
                    [
                        "tax provision",
                        "tax expense",
                        "income tax expense",
                        "provision for income taxes",
                    ],
                ),
                interest_expense=_get_float(
                    row_data,
                    [
                        "interest expense",
                        "interest expenses",
                        "interest expense non operating",
                        "total interest expense",
                    ],
                ),
                diluted_eps=_get_float(
                    row_data,
                    [
                        "diluted eps",
                        "diluted earnings per share",
                        "earnings per share diluted",
                    ],
                ),
                basic_eps=_get_float(row_data, ["basic eps", "basic earnings per share"]),
                # Cash Flow
                operating_cash_flow=_get_float(
                    row_data,
                    ["operating cash flow", "total cash from operating activities"],
                ),
                # Note: Yahoo returns Capex as NEGATIVE numbers.
                # We keep it raw here.
                capital_expenditure=_get_float(
                    row_data,
                    [
                        "capital expenditure",
                        "capital expenditures",
                        "purchase of property, plant and equipment",
                        "purchase of ppe",
                    ],
                ),
                free_cash_flow=_get_float(row_data, ["free cash flow"]),
                cash_dividends_paid=_get_float(
                    row_data,
                    [
                        "cash dividends paid",
                        "dividends paid",
                        "common stock dividends paid",
                    ],
                ),
                # Shares
                basic_average_shares=_get_float(
                    row_data, ["basic average shares", "ordinary shares number"]
                ),
                diluted_average_shares=_get_float(
                    row_data, ["diluted average shares", "ordinary shares number"]
                ),
                # Balance Sheet
                total_assets=_get_float(row_data, ["total assets"]),
                total_current_liabilities=_get_float(
                    row_data, ["total current liabilities", "current liabilities"]
                ),
                total_equity=_get_float(
                    row_data,
                    [
                        "total equity",
                        "stockholders equity",
                        "total stockholder equity",
                        "total equity and gross minority interest",
                    ],
                ),
                long_term_debt=_get_float(
                    row_data,
                    [
                        "long term debt",
                        "long-term debt",
                        "long term debt and capital lease obligations",
                    ],
                ),
                short_term_debt=_get_float(
                    row_data,
                    [
                        "current debt",
                        "current debt and capital lease obligations",
                        "commercial paper",
                        "short term debt",
                    ],
                ),
                total_debt=_get_float(
                    row_data,
                    [
                        "total debt",
                        "total debt and capital lease obligations",
                        "debt",
                    ],
                ),
                cash_and_equivalents=_get_float(row_data, ["cash and cash equivalents", "cash"]),
                goodwill=_get_float(row_data, ["goodwill"]),
                intangible_assets=_get_float(
                    row_data, ["intangible assets", "other intangible assets"]
                ),
                goodwill_and_other_intangible_assets=_get_float(
                    row_data, ["goodwill and other intangible assets"]
                ),
                share_issued=_get_float(
                    row_data,
                    [
                        "share issued",
                        "shares issued",
                        "ordinary shares number",
                        "common shares outstanding",
                        "common stock shares outstanding",
                    ],
                ),
            )

            # Quality check: skip reports with future dates
            has_pnl = (report.revenue is not None) or (report.net_income is not None)
            has_balance = (report.total_assets is not None) or (report.total_equity is not None)

            if not (has_pnl or has_balance):
                logger.warning(f"Skipping empty report for {ticker} on {parsed_date}")
                continue

            reports.append(report)
            logger.debug(f"Mapped {report_type} report for {ticker} on {parsed_date}")

        except Exception as e:
            logger.error(f"Failed to map report for {ticker} at {report_date}: {e}")
            continue

    logger.info(f"Mapped {len(reports)} {report_type} reports for {ticker}")
    return reports


def _get_float(data: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = data.get(key, None)
        if pd.notna(value):
            try:
                if isinstance(value, str):
                    return float(value.replace(",", ""))
                return float(value)
            except (ValueError, TypeError):
                continue
    return None


def _safe_date(data: dict[str, Any], keys: list[str]) -> date | None:
    lookup_map = {idx.strip().lower(): idx for idx in data.keys() if isinstance(idx, str)}
    for key in keys:
        key_clean = key.strip().lower()
        if key_clean in lookup_map:
            original_key = lookup_map[key_clean]
            value = data.get(original_key, None)
        # 1. Handle Nulls / NaNs
        if value is None:
            continue
        if isinstance(value, float) and (pd.isna(value) or value != value):
            continue

        # 2. Handle Lists (unpack first element)
        if isinstance(value, list):
            if not value:
                continue
            value = value[0]

        # 3. Try parsing whatever is left (String, Timestamp, Int, etc.)
        try:
            # pandas to_datetime is the most robust parser we have
            dt_val = pd.to_datetime(value)

            # Check if the result is valid (not NaT)
            if pd.notna(dt_val):
                return dt_val.date()  # type: ignore[no-any-return]
        except (ValueError, TypeError):
            continue

    return None


def _safe_str(data: dict[str, str], keys: list[str]) -> str | None:
    """
    Extract string value from pandas Series using multiple possible keys.
    Due to variations in yfinance column names, we try several options.
    The matching is case-insensitive and ignores leading/trailing whitespace.

    Args:
        row: pandas Series (one row from transposed DataFrame)
        keys: List of possible column names to try (case-insensitive)

    Returns:
        String value if found, None otherwise
    """
    # Create lowercase index for case-insensitive lookup
    lookup_map = {idx.strip().lower(): idx for idx in data.keys() if isinstance(idx, str)}

    for key in keys:
        key_clean = key.strip().lower()
        if key_clean in lookup_map:
            original_key = lookup_map[key_clean]
            value = data.get(original_key, None)
            if pd.notna(value):
                return str(value)
    return None
