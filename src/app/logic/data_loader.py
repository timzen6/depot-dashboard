"""Global data loader with caching for Streamlit application.

Handles loading and enriching price and fundamental data for the UI.
Uses Streamlit caching to avoid reloading data on every interaction.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.metrics import MetricsEngine
from src.config.settings import load_config


@dataclass
class DashboardData:
    """Container for dashboard data."""

    prices: pl.DataFrame
    fundamentals: pl.DataFrame
    metadata: pl.DataFrame

    fundamentals_quarterly: pl.DataFrame | None = None


class GlobalDataLoader:
    """Centralized data loader with metric calculation.

    Loads parquet files from production directories and enriches them
    with calculated metrics using the MetricsEngine.
    """

    def __init__(self, config_path: Path = Path("config/config.yaml")) -> None:
        """Initialize with configuration.

        Args:
            config_path: Path to main configuration file
        """
        self.config = load_config(config_path)
        self.metrics_engine = MetricsEngine()

    def load_data(self) -> DashboardData:
        """Load and enrich price and fundamental data.

        Loads raw parquet files and calculates derived metrics.
        Uses Streamlit caching via helper function to prevent reloads.

        Returns:
            Tuple of (prices, fundamentals) DataFrames with calculated metrics
        """
        raw_data = _load_cached_raw_data(
            self.config.settings.metadata_dir,
            self.config.settings.prices_dir,
            self.config.settings.fundamentals_dir,
        )

        prices, fundamentals = _calculate_metrics(
            raw_data.prices, raw_data.fundamentals, self.metrics_engine
        )

        return DashboardData(
            prices=prices,
            fundamentals=fundamentals,
            metadata=raw_data.metadata,
        )


def _calculate_metrics(
    prices: pl.DataFrame, fundamentals: pl.DataFrame, metrics_engine: MetricsEngine
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Helper to calculate metrics on loaded data.

    Args:
        prices: Raw price DataFrame
        fundamentals: Raw fundamental DataFrame
        metrics_engine: Engine for calculating derived metrics

    Returns:
        Tuple of (prices, fundamentals) DataFrames with calculated metrics
    """
    if not fundamentals.is_empty():
        fundamentals = metrics_engine.calculate_fundamental_metrics(fundamentals)
        fundamentals = metrics_engine.calculate_growth_metrics(
            fundamentals,
            metric_columns=[
                "revenue",
                "net_income",
                "diluted_eps",
                "basic_eps",
            ],
        )

    # Enrich prices with valuation metrics if we have both datasets
    if not prices.is_empty() and not fundamentals.is_empty():
        prices = metrics_engine.calculate_valuation_metrics(prices, fundamentals)
        logger.info("Calculated valuation metrics for price data")
        prices = metrics_engine.calculate_fair_value_history(prices, fundamentals, years=5)
        logger.info("Calculated fair value history for price data")

    return prices, fundamentals


@st.cache_data(ttl=3600, show_spinner="Loading data...")  # type: ignore[misc]
def _load_cached_raw_data(
    metadata_dir: Path,
    prices_dir: Path,
    fundamentals_dir: Path,
) -> DashboardData:
    """Cached helper to load and enrich data.

    Separated from class to work cleanly with Streamlit's caching decorator.
    Cache expires after 1 hour to allow for data updates.

    Args:
        prices_dir: Directory containing price parquet files
        fundamentals_dir: Directory containing fundamental parquet files

    Returns:
        Tuple of (prices, fundamentals) DataFrames
    """
    logger.info("Loading price and fundamental data from disk")
    # Load metadata
    df_metadata = pl.read_parquet(metadata_dir / "asset_metadata.parquet")

    # Load all price files
    price_files = sorted(prices_dir.glob("*.parquet"))
    if not price_files:
        logger.warning(f"No price files found in {prices_dir}")
        df_prices = pl.DataFrame()
    else:
        df_prices = pl.concat([pl.read_parquet(f) for f in price_files], how="vertical_relaxed")
        logger.info(f"Loaded {df_prices.height:,} price records from {len(price_files)} files")

    annual_path = fundamentals_dir / "annual"
    if not annual_path.exists():
        annual_path = fundamentals_dir

    # Load all fundamental files
    annual_files = sorted(annual_path.glob("*.parquet"))
    if not annual_files:
        logger.warning(f"No fundamental files found in {annual_path}")
        df_annual = pl.DataFrame()
    else:
        df_annual = pl.concat(
            [pl.read_parquet(f) for f in annual_files],
            # how="vertical_relaxed",
            how="diagonal",
        )
        logger.info(
            f"Loaded {df_annual.height:,} fundamental records from {len(annual_files)} files"
        )

    quaterly_path = fundamentals_dir / "quarterly"
    df_quarterly = None

    if quaterly_path.exists():
        try:
            quarterly_files = sorted(quaterly_path.glob("*.parquet"))
            if quarterly_files:
                df_quarterly = pl.concat(
                    [pl.read_parquet(f) for f in quarterly_files],
                    # how="vertical_relaxed",
                    how="diagonal",
                )
                logger.info(
                    f"Loaded {df_quarterly.height:,} quarterly fundamental"
                    f" records from {len(quarterly_files)} files"
                )
        except Exception as e:
            logger.error(f"Error loading quarterly fundamentals: {e}")

    return DashboardData(
        prices=df_prices,
        fundamentals=df_annual,
        metadata=df_metadata,
        fundamentals_quarterly=df_quarterly,
    )
