"""Quality Core - Main Entry Point with CLI Commands.

Supports:
- etl: Run data extraction and storage pipeline
- snapshot: Create compressed backup snapshots
- restore: Restore data from snapshots
"""

import argparse
import sys
from pathlib import Path

from loguru import logger

from src.config.settings import load_config
from src.core.file_manager import ParquetStorage
from src.data_mgmt.archiver import DataArchiver
from src.etl.extract import DataExtractor
from src.etl.pipeline import ETLPipeline


def cmd_etl(args: argparse.Namespace) -> None:
    """Run ETL pipeline for prices and fundamentals."""
    logger.info("=== Running ETL Pipeline ===")

    # Load configuration
    config = load_config()

    # Initialize storage
    prices_storage = ParquetStorage(config.settings.prices_dir)
    fundamentals_storage = ParquetStorage(config.settings.fundamentals_dir)

    # Initialize extractor
    extractor = DataExtractor()

    # Run price updates for ALL tickers (universe + portfolios)
    total_tickers = config.all_tickers
    logger.info(f"Updating prices for {len(total_tickers)} tickers (universe + portfolios)")
    price_pipeline = ETLPipeline(prices_storage, extractor)
    price_pipeline.run_price_update(total_tickers)

    # Run fundamental updates only for stocks (not FX/crypto)
    stock_tickers = config.all_stock_tickers
    logger.info(f"Updating fundamentals for {len(stock_tickers)} stocks")
    fundamental_pipeline = ETLPipeline(fundamentals_storage, extractor)
    fundamental_pipeline.run_fundamental_update(stock_tickers)
    logger.success("✅ ETL Pipeline completed successfully")


def cmd_snapshot(args: argparse.Namespace) -> None:
    """Create compressed snapshots of price and fundamental data."""
    logger.info("=== Creating Data Snapshots ===")

    # Load configuration
    config = load_config()

    # Initialize archiver
    archiver = DataArchiver(config.settings.base_dir, config.settings.archive_dir)

    # Create snapshots for both data types
    data_types = args.data_type if args.data_type else ["prices", "fundamentals"]

    for data_type in data_types:
        try:
            snapshot_path = archiver.create_snapshot(data_type)
            logger.success(f"Created {data_type} snapshot: {snapshot_path}")
        except Exception as e:
            logger.error(f"Failed to create {data_type} snapshot: {e}")
            sys.exit(1)


def cmd_restore(args: argparse.Namespace) -> None:
    """Restore data from snapshot file(s)."""
    logger.info("=== Restoring Data from Snapshot ===")

    # Load configuration
    config = load_config()

    # Initialize archiver
    archiver = DataArchiver(config.settings.base_dir, config.settings.archive_dir)

    # High-level mode: Restore latest snapshots for both prices and fundamentals
    if args.snapshot_file is None:
        logger.info("High-level restore mode: Restoring latest prices and fundamentals snapshots")

        target_base = Path(args.target_dir) if args.target_dir else config.settings.base_dir

        # Restore prices
        price_snapshots = archiver.list_snapshots("prices")
        if not price_snapshots:
            logger.warning("No prices snapshots found, skipping prices restore")
        else:
            latest_prices = price_snapshots[0]
            target_prices = target_base / "prices"
            logger.info(f"Restoring prices from {latest_prices.name}")
            try:
                archiver.restore_snapshot(latest_prices, target_prices)
                logger.success(f"✅ Prices restored to {target_prices}")
            except Exception as e:
                logger.error(f"Failed to restore prices: {e}")
                sys.exit(1)

        # Restore fundamentals
        fund_snapshots = archiver.list_snapshots("fundamentals")
        if not fund_snapshots:
            logger.warning("No fundamentals snapshots found, skipping fundamentals restore")
        else:
            latest_fund = fund_snapshots[0]
            target_fund = target_base / "fundamentals"
            logger.info(f"Restoring fundamentals from {latest_fund.name}")
            try:
                archiver.restore_snapshot(latest_fund, target_fund)
                logger.success(f"✅ Fundamentals restored to {target_fund}")
            except Exception as e:
                logger.error(f"Failed to restore fundamentals: {e}")
                sys.exit(1)

        logger.success(f"✅ All snapshots restored to {target_base}")
        return

    # Low-level mode: Restore specific snapshot file
    snapshot_path = Path(args.snapshot_file)

    if args.target_dir:
        target_dir = Path(args.target_dir)
    else:
        # Auto-detect from snapshot name
        if "prices" in snapshot_path.name:
            target_dir = config.settings.prices_dir
        elif "fundamentals" in snapshot_path.name:
            target_dir = config.settings.fundamentals_dir
        else:
            logger.error("Cannot auto-detect target directory. Use --target-dir")
            sys.exit(1)

    try:
        archiver.restore_snapshot(snapshot_path, target_dir)
        logger.success(f"✅ Restored snapshot to {target_dir}")
    except Exception as e:
        logger.error(f"Failed to restore snapshot: {e}")
        sys.exit(1)


def cmd_list_snapshots(args: argparse.Namespace) -> None:
    """List available snapshots."""
    config = load_config()
    archiver = DataArchiver(config.settings.base_dir, config.settings.archive_dir)

    snapshots = archiver.list_snapshots(args.data_type)

    if not snapshots:
        logger.info("No snapshots found")
        return

    logger.info(f"Found {len(snapshots)} snapshot(s):")
    for snapshot in snapshots:
        size_mb = snapshot.stat().st_size / 1024 / 1024
        logger.info(f"  • {snapshot.name} ({size_mb:.2f} MB)")


def main() -> None:
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Quality Core - Data Pipeline and Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # ETL command
    parser_etl = subparsers.add_parser("etl", help="Run data extraction and storage pipeline")
    parser_etl.set_defaults(func=cmd_etl)

    # Snapshot command
    parser_snapshot = subparsers.add_parser("snapshot", help="Create compressed data snapshots")
    parser_snapshot.add_argument(
        "--data-type",
        nargs="+",
        choices=["prices", "fundamentals"],
        help="Data type(s) to snapshot (default: both)",
    )
    parser_snapshot.set_defaults(func=cmd_snapshot)

    # Restore command
    parser_restore = subparsers.add_parser(
        "restore",
        help="Restore data from snapshot(s)",
        description=(
            "High-level mode (default): Restores latest prices and fundamentals snapshots\n"
            "Low-level mode: Restores a specific snapshot file (use --snapshot-file)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser_restore.add_argument(
        "--snapshot-file",
        type=str,
        help="Path to specific snapshot file for low-level restore",
    )
    parser_restore.add_argument(
        "--target-dir",
        type=str,
        help=(
            "Target base directory. In high-level mode, creates prices/ and fundamentals/ "
            "subdirs. In low-level mode, specifies exact target dir."
        ),
    )
    parser_restore.set_defaults(func=cmd_restore)

    # List snapshots command
    parser_list = subparsers.add_parser("list", help="List available snapshots")
    parser_list.add_argument(
        "--data-type", choices=["prices", "fundamentals"], help="Filter by data type"
    )
    parser_list.set_defaults(func=cmd_list_snapshots)

    # Parse arguments and execute
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
