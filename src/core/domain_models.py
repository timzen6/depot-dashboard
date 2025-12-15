from datetime import date
from enum import Enum

import polars as pl
from pydantic import BaseModel, ConfigDict

# --- Constants & Schemas ---

# Polars Schema for high-performance IO of price data
# We use this instead of instantiating thousands of Pydantic objects for prices.
STOCK_PRICE_SCHEMA = {
    "ticker": pl.Utf8,
    "date": pl.Date,
    "currency": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Int64,
    "dividend": pl.Float64,
}


# --- Enums ---


class ReportType(str, Enum):
    """Distinguishes between Annual (10-K) and Quarterly (10-Q) reports."""

    ANNUAL = "annual"
    QUARTERLY = "quarterly"


# --- Domain Models ---


class StockPrice(BaseModel):
    """
    Represents a single day's trading data.

    Note: For bulk processing (ETL), prefer using Polars DataFrames
    with `STOCK_PRICE_SCHEMA` to avoid serialization overhead.
    This model is primarily for single-record API responses or strict validation.
    """

    model_config = ConfigDict(frozen=True)

    ticker: str
    date: date
    currency: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int
    dividend: float = 0.0


class FinancialReport(BaseModel):
    """
    Core domain model representing a company's financial state at a specific point in time.
    Includes fields necessary for ROCE, FCF, and TTM calculations.

    Design Choice:
    - All financial fields are Optional[float] to handle data gaps from providers.
    - Derived metrics (like capital_employed) are computed properties to ensure consistency.
    """

    model_config = ConfigDict(frozen=True)

    # Metadata
    ticker: str
    report_date: date
    period_type: ReportType
    currency: str = "USD"

    # Income Statement
    revenue: float | None = None
    gross_profit: float | None = None
    ebit: float | None = None  # Crucial for ROCE
    net_income: float | None = None
    tax_provision: float | None = None

    # Per Share Data
    diluted_eps: float | None = None
    basic_eps: float | None = None

    # Cash Flow
    operating_cash_flow: float | None = None
    capital_expenditure: float | None = None  # Crucial for FCF
    free_cash_flow: float | None = None  # Often provided directly, but verify with OCF - Capex

    # Shares
    # We need this to calculate market-cap based metrics
    basic_average_shares: float | None = None  # For per-share metrics
    diluted_average_shares: float | None = None

    # Balance Sheet (Snapshot at report_date)
    total_assets: float | None = None
    total_current_liabilities: float | None = None
    total_equity: float | None = None
    long_term_debt: float | None = None
    cash_and_equivalents: float | None = None

    @property
    def capital_employed(self) -> float | None:
        """
        Proxy for Capital Employed calculation.
        Formula: Total Assets - Current Liabilities
        Used for ROCE (Return on Capital Employed).
        """
        if self.total_assets is not None and self.total_current_liabilities is not None:
            return self.total_assets - self.total_current_liabilities
        return None

    @property
    def net_debt(self) -> float | None:
        """
        Proxy for Net Debt.
        Formula: Long Term Debt - Cash & Equivalents
        (Simplified view; often Short Term Debt is also included).
        """
        if self.long_term_debt is not None and self.cash_and_equivalents is not None:
            return self.long_term_debt - self.cash_and_equivalents
        return None
