"""Configuration management for Quality Core.

Centralizes all application settings and ticker universe definitions.
"""

from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from pydantic import BaseModel, Field

from src.config.models import PortfoliosConfig


class UniverseConfig(BaseModel):
    """Ticker universe configuration."""

    stocks: list[str] = Field(description="Tickers with fundamental data")
    price_only: list[str] = Field(description="Tickers with price data only (FX, ETFs, indices)")

    @property
    def all_tickers(self) -> list[str]:
        """Return all tickers (stocks + price-only assets)."""
        return self.stocks + self.price_only


class AppSettings(BaseModel):
    """Application-level settings."""

    base_dir: Path = Field(default=Path("data/prod"))
    archive_dir: Path = Field(default=Path("data/archive"))
    initial_price_start_date: str = Field(default="2021-01-01")

    @property
    def prices_dir(self) -> Path:
        """Directory for price data."""
        return self.base_dir / "prices"

    @property
    def fundamentals_dir(self) -> Path:
        """Directory for fundamental data."""
        return self.base_dir / "fundamentals"

    @property
    def metadata_dir(self) -> Path:
        """Directory for asset metadata."""
        return self.base_dir / "metadata"


class Config(BaseModel):
    """Root configuration model."""

    universe: UniverseConfig
    settings: AppSettings
    portfolios: PortfoliosConfig | None = Field(
        default=None, description="Portfolio configurations"
    )

    @property
    def all_tickers(self) -> list[str]:
        """Merged ticker list: universe + portfolios.

        Automatically includes tickers from portfolio definitions to ensure
        ETL pipeline fetches all necessary data.
        """
        # Start with universe tickers
        tickers = set(self.universe.all_tickers)

        # Add portfolio tickers if portfolios config exists
        if self.portfolios:
            tickers.update(self.portfolios.all_tickers)

        return sorted(list(tickers))


def load_config(config_path: Path = Path("config.yaml")) -> Config:
    """Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml file

    Returns:
        Parsed configuration object

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is malformed
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    logger.info(f"Loading configuration from {config_path}")

    with config_path.open("r") as f:
        raw_config: dict[str, Any] = yaml.safe_load(f)

    # Load portfolios config if it exists
    portfolios_path = Path("portfolios.yaml")
    portfolios_config = None

    if portfolios_path.exists():
        logger.info(f"Loading portfolios from {portfolios_path}")
        with portfolios_path.open("r") as f:
            portfolios_raw: dict[str, Any] = yaml.safe_load(f)
            portfolios_config = PortfoliosConfig(**portfolios_raw)
            logger.debug(f"Loaded {len(portfolios_config.portfolios)} portfolios")

    # Merge into main config
    raw_config["portfolios"] = portfolios_config

    config = Config(**raw_config)
    logger.debug(f"Total tickers (universe + portfolios): {len(config.all_tickers)}")

    return config
