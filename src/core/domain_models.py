from datetime import date
from enum import StrEnum
from typing import Any

import polars as pl
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, model_validator

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


class ReportType(StrEnum):
    """Distinguishes between Annual (10-K) and Quarterly (10-Q) reports."""

    ANNUAL = "annual"
    QUARTERLY = "quarterly"


class AssetType(StrEnum):
    """Type of financial asset."""

    STOCK = "stock"  # Does need financials
    ETF = "etf"  # Does need info about holdings and costs
    FX = "fx"  # Just price data
    COMMODITY = "commodity"  # Just price data (e.g. gold, silver, oil)
    CRYPTO = "crypto"  # Optional, also just price data


class Sector(StrEnum):
    """Industry sectors based on GICS classification."""

    TECHNOLOGY = "Technology"
    HEALTHCARE = "Healthcare"
    FINANCIALS = "Financials"
    CONSUMER_DISCRETIONARY = "Consumer Discretionary"
    COMMUNICATION = "Communication"
    INDUSTRIALS = "Industrials"
    CONSUMER_STAPLES = "Consumer Staples"
    ENERGY = "Energy"
    UTILITIES = "Utilities"
    REAL_ESTATE = "Real Estate"
    MATERIALS = "Materials"
    OTHER = "Other"


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
    interest_expense: float | None = None

    # Per Share Data
    diluted_eps: float | None = None
    basic_eps: float | None = None

    # Cash Flow
    operating_cash_flow: float | None = None
    capital_expenditure: float | None = None  # Crucial for FCF
    free_cash_flow: float | None = None  # Often provided directly, but verify with OCF - Capex

    cash_dividends_paid: float | None = None

    # Shares
    # We need this to calculate market-cap based metrics
    basic_average_shares: float | None = None  # For per-share metrics
    diluted_average_shares: float | None = None
    share_issued: float | None = None

    # Balance Sheet (Snapshot at report_date)
    total_assets: float | None = None
    total_current_liabilities: float | None = None
    total_equity: float | None = None
    long_term_debt: float | None = None
    short_term_debt: float | None = None
    total_debt: float | None = None
    cash_and_equivalents: float | None = None
    goodwill: float | None = None
    intangible_assets: float | None = None
    # Sometimes a combined field is provided directly
    goodwill_and_other_intangible_assets: float | None = None

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
        debt = self.total_debt

        if debt is None:
            ltd = self.long_term_debt or 0.0
            std = self.short_term_debt or 0.0
            if self.long_term_debt is not None or self.short_term_debt is not None:
                debt = ltd + std
        if debt is not None and self.cash_and_equivalents is not None:
            return debt - self.cash_and_equivalents
        return None

    @property
    def tangible_book_value(self) -> float | None:
        """
        Proxy for Tangible Book Value.
        Formula: Total Equity - Goodwill - Intangible Assets
        """
        if self.total_equity is None:
            return None

        intangibles = self.goodwill_and_other_intangible_assets
        if intangibles is None:
            goodwill = self.goodwill or 0.0
            intangible_assets = self.intangible_assets or 0.0
            intangibles = goodwill + intangible_assets
        return self.total_equity - intangibles


class AssetMetadata(BaseModel):
    """Metadata about a financial asset."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    name: str
    asset_type: AssetType
    currency: str = "USD"
    short_name: str | None = None
    exchange: str | None = None
    sector_raw: str | None = None
    sector: Sector | None = None
    industry: str | None = None
    country: str | None = None

    display_name: str | None = None
    forward_pe: float | None = None
    forward_eps: float | None = None

    dividend_date: date | None = None
    earnings_date: date | None = None

    last_updated: date | None = None

    def to_dict(self) -> dict[str, str | None]:
        """Convert AssetMetadata to a dictionary for easy serialization."""
        # just use the model dump
        data = self.model_dump()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AssetMetadata":
        """Create AssetMetadata from a dictionary relying on Pydantic validation."""
        try:
            return cls.model_validate(data)
        except Exception as e:
            logger.error(f"Failed to parse AssetMetadata from dict: {e}")
            raise e


class AllocationItem(BaseModel):
    """Represents a single allocation item in a etf."""

    model_config = ConfigDict(frozen=True)
    category: str
    weight: float = Field(..., ge=0.0, le=1.0, description="Weight as a percentage (0-100)")


class ETFHolding(BaseModel):
    """Represents a single holding within an ETF."""

    model_config = ConfigDict(frozen=True)

    name: str
    ticker: str | None = None
    weight: float = Field(..., ge=0.0, le=1.0, description="Weight as a percentage (0-100)")


class ETFComposition(BaseModel):
    """Represents the holdings of an ETF."""

    model_config = ConfigDict(frozen=True)

    ticker: str
    name: str
    ter: float = Field(default=0.0, description="Total Expense Ratio as a percentage")
    strategy: str | None = Field(
        default=None,
        description=(
            "Description of the ETF's investment strategy"
            " e.g. Global Large Cap, Thematic, Bond-focused"
        ),
    )

    sector_weights: list[AllocationItem] = Field(
        default_factory=list, description="List of sector allocations"
    )
    country_weights: list[AllocationItem] = Field(
        default_factory=list, description="List of country allocations"
    )
    top_holdings: list[ETFHolding] = Field(
        default_factory=list, description="List of top holdings in the ETF"
    )

    @model_validator(mode="after")
    def validate_coverage(self) -> "ETFComposition":
        """
        Validates that allocations do not exceed 105% (allowing for small rounding errors/cash).
        Also warns if coverage is significantly low (< 90%).
        """
        max_tolerance = 1.05  # 105% erlaubt wegen Rundung/Cash
        min_warning = 0.90  # Unter 90% gibt es eine Warnung im Log

        # 1. Validate Sectors
        total_sectors = self.total_sector_coverage
        if total_sectors > max_tolerance:
            raise ValueError(
                f"❌ Data Error in {self.ticker}: "
                f" Sector weights sum to {total_sectors:.1%} (Limit: {max_tolerance:.0%})"
            )
        if total_sectors < min_warning and self.sector_weights:
            logger.warning(
                f"⚠️  Data Warning {self.ticker}: "
                f" Sector coverage only {total_sectors:.1%} (Missing data?)"
            )

        # 2. Validate Countries
        total_countries = self.total_country_coverage
        if total_countries > max_tolerance:
            raise ValueError(
                f"❌ Data Error in {self.ticker}: "
                f" Country weights sum to {total_countries:.1%} (Limit: {max_tolerance:.0%})"
            )
        if total_countries < min_warning and self.country_weights:
            logger.warning(
                f"⚠️  Data Warning {self.ticker}: "
                f" Country coverage only {total_countries:.1%} (Missing data?)"
            )

        # 3. Validate Holdings (Nur Max-Check, da Holdings selten 100% sind)
        total_holdings = self.total_top_holdings_coverage
        if total_holdings > max_tolerance:
            raise ValueError(
                f"❌ Data Error in {self.ticker}: "
                f"Top Holdings sum to {total_holdings:.1%}! Did you mix up % and ratio?"
            )

        return self

    @property
    def total_sector_coverage(self) -> float:
        """Calculate total sector coverage percentage."""
        return sum(item.weight for item in self.sector_weights)

    @property
    def total_country_coverage(self) -> float:
        """Calculate total country coverage percentage."""
        return sum(item.weight for item in self.country_weights)

    @property
    def total_top_holdings_coverage(self) -> float:
        """Calculate total coverage percentage of top holdings."""
        return sum(holding.weight for holding in self.top_holdings)

    @property
    def sectors_df(self) -> pl.DataFrame:
        """Return sector allocations as a Polars DataFrame."""
        if not self.sector_weights:
            return pl.DataFrame(
                schema={
                    "ticker": pl.Utf8,
                    "category": pl.Utf8,
                    "weight": pl.Float64,
                }
            )
        return pl.DataFrame(
            [
                {
                    "ticker": self.ticker,
                    "category": item.category,
                    "weight": item.weight,
                }
                for item in self.sector_weights
            ]
        )

    @property
    def countries_df(self) -> pl.DataFrame:
        """Return country allocations as a Polars DataFrame."""
        if not self.country_weights:
            return pl.DataFrame(
                schema={
                    "ticker": pl.Utf8,
                    "category": pl.Utf8,
                    "weight": pl.Float64,
                }
            )
        return pl.DataFrame(
            [
                {
                    "ticker": self.ticker,
                    "category": item.category,
                    "weight": item.weight,
                }
                for item in self.country_weights
            ]
        )

    @property
    def top_holdings_df(self) -> pl.DataFrame:
        """Return top holdings as a Polars DataFrame."""
        if not self.top_holdings:
            return pl.DataFrame(
                schema={
                    "etf_ticker": pl.Utf8,
                    "holding_ticker": pl.Utf8,
                    "holding_name": pl.Utf8,
                    "weight": pl.Float64,
                }
            )
        return pl.DataFrame(
            [
                {
                    "etf_ticker": self.ticker,
                    "holding_ticker": holding.ticker,
                    "holding_name": holding.name,
                    "weight": holding.weight,
                }
                for holding in self.top_holdings
            ]
        )
