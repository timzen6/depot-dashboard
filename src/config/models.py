"""Pydantic models for portfolio configuration."""

from datetime import date as date_type
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class PortfolioType(str, Enum):
    """Portfolio strategy type."""

    WEIGHTED = "weighted"
    ABSOLUTE = "absolute"
    WATCHLIST = "watchlist"


class Position(BaseModel):
    """Individual position in a portfolio."""

    ticker: str = Field(description="Stock ticker symbol")
    weight: float | None = Field(
        default=None, description="Portfolio weight (for weighted portfolios)"
    )
    shares: float | None = Field(
        default=None, description="Number of shares (for absolute portfolios)"
    )
    group: str | None = Field(default=None, description="Optional group/category for the position")

    @field_validator("weight")
    @classmethod
    def validate_weight(cls, v: float | None) -> float | None:
        """Only ensure weight is not negative if provided."""
        if v is not None and v < 0:
            raise ValueError("Weight must not be negative")
        return v

    @field_validator("shares")
    @classmethod
    def validate_shares(cls, v: float | None) -> float | None:
        """Ensure shares are positive if provided."""
        if v is not None and v <= 0:
            raise ValueError("Shares must be positive")
        return v


class Portfolio(BaseModel):
    """Portfolio configuration."""

    name: str = Field(description="Portfolio identifier")
    display_name: str | None = Field(default=None, description="Optional display name for UI")
    type: PortfolioType = Field(description="Portfolio strategy type")
    start_date: str | None = Field(
        default=None, description="Portfolio inception date (YYYY-MM-DD)"
    )
    initial_capital: float | None = Field(
        default=None, description="Starting capital (for weighted portfolios)"
    )
    positions: list[Position] = Field(description="List of positions")
    is_editable: bool = Field(
        default=False, description="Whether the portfolio can be edited via the UI"
    )

    @property
    def tickers(
        self,
    ) -> list[str]:
        """Extract all tickers from positions, optionally filtered by type."""
        return list({pos.ticker for pos in self.positions})

    @property
    def ui_name(self) -> str:
        """Get display name for UI, defaulting to name if not set."""
        return self.display_name or self.name

    @field_validator("initial_capital")
    @classmethod
    def validate_capital(cls, v: float | None, info: ValidationInfo[Any]) -> float | None:
        """Ensure initial_capital is provided for weighted portfolios."""
        portfolio_type = info.data.get("type")
        if portfolio_type == PortfolioType.WEIGHTED and v is None:
            raise ValueError("initial_capital is required for weighted portfolios")
        if v is not None and v <= 0:
            raise ValueError("initial_capital must be positive")
        return v

    @field_validator("start_date")
    @classmethod
    def validate_start_date_format(cls, v: str | None, info: ValidationInfo[Any]) -> str | None:
        """Ensure start_date is provided for weighted portfolios and has valid format."""
        portfolio_type = info.data.get("type")
        if portfolio_type == PortfolioType.WEIGHTED and v is None:
            raise ValueError("start_date is required for weighted portfolios")
        if v is not None:
            try:
                # Validate YYYY-MM-DD format
                date_type.fromisoformat(v)
            except ValueError:
                raise ValueError(f"start_date must be in YYYY-MM-DD format, got: {v}") from None
        return v

    def model_post_init(self, __context: Any) -> None:
        """Validate portfolio-specific constraints after initialization."""
        # Weighted portfolios: validate weights sum to 1.0
        if self.type == PortfolioType.WEIGHTED:
            # Ensure all positions have weights
            if any(pos.weight is None for pos in self.positions):
                raise ValueError("All positions must have weights in weighted portfolios")

        # Absolute portfolios: ensure all positions have shares
        if self.type == PortfolioType.ABSOLUTE:
            if any(pos.shares is None for pos in self.positions):
                raise ValueError("All positions must have shares in absolute portfolios")


class PortfoliosConfig(BaseModel):
    """Root configuration for portfolios."""

    portfolios: dict[str, Portfolio] = Field(description="Portfolio definitions")

    def __len__(self) -> int:
        return len(self.portfolios)

    @field_validator("portfolios", mode="before")
    @classmethod
    def set_portfolio_names(cls, v: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Inject portfolio name from dictionary key."""
        for name, portfolio_data in v.items():
            if isinstance(portfolio_data, dict):
                portfolio_data["name"] = name
            # If it is already a Portfolio instance, name is set
            # No need to do anything
        return v

    @property
    def all_tickers(
        self,
    ) -> list[str]:
        tickers = set()
        for portfolio in self.portfolios.values():
            tickers.update(portfolio.tickers)
        return list(tickers)
