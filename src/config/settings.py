"""Configuration management for Quality Core.

Centralizes all application settings and ticker universe definitions.
"""

from pathlib import Path
from typing import Any

import yaml
from loguru import logger
from pydantic import BaseModel, Field


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


class Config(BaseModel):
    """Root configuration model."""

    universe: UniverseConfig
    settings: AppSettings


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

    config = Config(**raw_config)
    logger.debug(f"Loaded {len(config.universe.all_tickers)} tickers from config")

    return config
