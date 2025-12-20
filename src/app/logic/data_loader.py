"""Global data loader with caching for Streamlit application.

Handles loading and enriching price and fundamental data for the UI.
Uses Streamlit caching to avoid reloading data on every interaction.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
from loguru import logger

from src.analysis.metrics import MetricsEngine
from src.config.settings import load_config


@dataclass
class DashboardData:
    """Container for dashboard data."""

    prices: pl.DataFrame
    fundamentals: pl.DataFrame
    metadata: pl.DataFrame


class GlobalDataLoader:
    """Centralized data loader with metric calculation.

    Loads parquet files from production directories and enriches them
    with calculated metrics using the MetricsEngine.
    """

    def __init__(self, config_path: Path = Path("config.yaml")) -> None:
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


# @st.cache_data(ttl=3600, show_spinner="Loading data...")  # type: ignore[misc]
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

    # Load all fundamental files
    fund_files = sorted(fundamentals_dir.glob("*.parquet"))
    if not fund_files:
        logger.warning(f"No fundamental files found in {fundamentals_dir}")
        df_fund = pl.DataFrame()
    else:
        df_fund = pl.concat([pl.read_parquet(f) for f in fund_files], how="vertical_relaxed")
        logger.info(f"Loaded {df_fund.height:,} fundamental records from {len(fund_files)} files")

    return DashboardData(
        prices=df_prices,
        fundamentals=df_fund,
        metadata=df_metadata,
    )
