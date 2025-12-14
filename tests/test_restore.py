"""Test for restore functionality - can be run in debug mode."""

from pathlib import Path

import polars as pl
from loguru import logger

from src.config.settings import load_config
from src.data_mgmt.archiver import DataArchiver


def test_high_level_restore() -> None:
    """Test high-level restore: restore latest snapshots to test directory.

    Equivalent to: uv run qc restore --target-dir data/test
    """
    # Load config
    config = load_config()

    # Initialize archiver
    archiver = DataArchiver(config.settings.base_dir, config.settings.archive_dir)

    # Define target base directory
    target_base = Path("data/test")

    # Restore prices
    logger.info("Restoring latest prices snapshot")
    price_snapshots = archiver.list_snapshots("prices")
    if price_snapshots:
        latest_prices = price_snapshots[0]
        target_prices = target_base / "prices"
        logger.info(f"Restoring prices from {latest_prices.name}")
        archiver.restore_snapshot(latest_prices, target_prices)
        logger.success(f"✅ Prices restored to {target_prices}")
    else:
        logger.warning("No prices snapshots found")

    # Restore fundamentals
    logger.info("Restoring latest fundamentals snapshot")
    fund_snapshots = archiver.list_snapshots("fundamentals")
    if fund_snapshots:
        latest_fund = fund_snapshots[0]
        target_fund = target_base / "fundamentals"
        logger.info(f"Restoring fundamentals from {latest_fund.name}")
        archiver.restore_snapshot(latest_fund, target_fund)
        logger.success(f"✅ Fundamentals restored to {target_fund}")
    else:
        logger.warning("No fundamentals snapshots found")

    logger.success(f"✅ All snapshots restored to {target_base}")

    # Read and verify restored price data for SU.PA and MSFT
    logger.info("Reading restored price data for SU.PA and MSFT")

    prices_dir = target_base / "prices"
    tickers: list[str] = ["SU.PA", "MSFT"]

    for ticker in tickers:
        ticker_path = prices_dir / f"{ticker}.parquet"
        if ticker_path.exists():
            df_test = pl.read_parquet(ticker_path)
            row_count = len(df_test)
            date_min = str(df_test["date"].min())
            date_max = str(df_test["date"].max())
            columns = list(df_test.columns)

            logger.info(f"{ticker}: {row_count:,} rows")
            logger.info(f"Date range: {date_min} to {date_max}")
            logger.info(f"Columns: {columns!r}")
            print(f"\n{ticker} sample:")
            print(df_test.head())
        else:
            logger.warning(f"{ticker} data not found at {ticker_path}")


if __name__ == "__main__":
    # Run the restore test for debugging
    test_high_level_restore()
