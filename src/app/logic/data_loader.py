"""Global data loader with caching for Streamlit application.

Handles loading and enriching price and fundamental data for the UI.
Uses Streamlit caching to avoid reloading data on every interaction.
"""

from dataclasses import dataclass
from pathlib import Path

import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.analysis.metrics import MetricsEngine
from src.config.models import Portfolio
from src.config.settings import load_config
from src.core.admin_engine import AdminEngine


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
        self.admin_engine = AdminEngine()

    def load_portfolios(self) -> dict[str, Portfolio]:
        """Load portfolio configurations.

        Returns:
            Dictionary of portfolio name to Portfolio objects
        """
        return self.admin_engine.portfolio_manager.get_all_portfolios()

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
            prices=raw_data.prices,
            fundamentals=raw_data.fundamentals,
            fundamentals_quarterly=raw_data.fundamentals_quarterly,
            metrics_engine=self.metrics_engine,
        )

        return DashboardData(
            prices=prices,
            fundamentals=fundamentals,
            metadata=raw_data.metadata,
            fundamentals_quarterly=raw_data.fundamentals_quarterly,
        )


def _calculate_metrics(
    prices: pl.DataFrame,
    fundamentals: pl.DataFrame,
    fundamentals_quarterly: pl.DataFrame | None,
    metrics_engine: MetricsEngine,
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
        prices = metrics_engine.calculate_valuation_metrics(
            prices,
            fundamentals,
            fundamentals_quarterly,
        )
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
        df_prices = pl.concat([pl.read_parquet(f) for f in price_files], how="diagonal_relaxed")
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
            how="diagonal_relaxed",
        )
        logger.info(
            f"Loaded {df_annual.height:,} fundamental records from {len(annual_files)} files"
        )

    quarterly_path = fundamentals_dir / "quarterly"
    df_quarterly = None

    if quarterly_path.exists():
        try:
            quarterly_files = sorted(quarterly_path.glob("*.parquet"))
            if quarterly_files:
                df_quarterly = pl.concat(
                    [pl.read_parquet(f) for f in quarterly_files],
                    how="diagonal_relaxed",
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


# we need the caching to stabilize the selection
@st.cache_data(ttl=3600, show_spinner="Loading data...")  # type: ignore[misc]
def load_all_stock_data() -> tuple[DashboardData, dict[str | None, list[str]], FXEngine]:
    # Load data
    try:
        loader = GlobalDataLoader()
        dashboard_data = loader.load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        logger.error(f"Data loading error: {e}", exc_info=True)
        raise e
    # Get available tickers
    if dashboard_data.prices.is_empty():
        raise (Exception("No price data available"))

    portfolio_dict_raw = loader.load_portfolios()
    portfolio_dict = {p.display_name: p.tickers for p in portfolio_dict_raw.values()}
    fx_engine = FXEngine(dashboard_data.prices, target_currency="EUR")

    return dashboard_data, portfolio_dict, fx_engine
