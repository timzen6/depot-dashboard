"""Data archiving and snapshot management.

Provides compressed monolithic snapshots of parquet data for:
- Backup and disaster recovery
- High-performance batch analysis
- Data versioning and reproducibility
"""

from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger


class DataArchiver:
    """Manages creation and restoration of compressed data snapshots."""

    def __init__(self, base_dir: Path, archive_dir: Path) -> None:
        """Initialize archiver with data and archive directories.

        Args:
            base_dir: Root directory containing prices/ and fundamentals/
            archive_dir: Directory for storing snapshot archives
        """
        self.base_dir = Path(base_dir)
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"DataArchiver initialized (archive: {self.archive_dir})")

    def create_snapshot(self, data_type: str) -> Path:
        """Create compressed monolithic snapshot of all parquet files.

        Optimization: Casts low-cardinality columns to Categorical to reduce memory.

        Args:
            data_type: Type of data ("prices" or "fundamentals")

        Returns:
            Path to created snapshot file

        Raises:
            ValueError: If data_type is invalid or no data found
        """
        if data_type not in ("prices", "fundamentals"):
            raise ValueError(f"Invalid data_type: {data_type}. Must be 'prices' or 'fundamentals'")

        source_dir = self.base_dir / data_type
        if not source_dir.exists():
            raise ValueError(f"Source directory does not exist: {source_dir}")

        pattern = str(source_dir / "*.parquet")
        logger.info(f"Creating {data_type} snapshot from {pattern}")

        # Read all parquet files in one go
        data = pl.read_parquet(pattern)

        if data.is_empty():
            raise ValueError(f"No data found in {source_dir}")

        # Optimize memory: Cast low-cardinality columns to Categorical
        categorical_cols = ["ticker", "currency"]
        if data_type == "fundamentals":
            categorical_cols.append("period_type")

        for col in categorical_cols:
            if col in data.columns:
                data = data.with_columns(pl.col(col).cast(pl.Categorical))

        # Generate snapshot filename with current date
        snapshot_date = date.today().isoformat()
        snapshot_filename = f"{data_type}_snapshot_{snapshot_date}.parquet"
        snapshot_path = self.archive_dir / snapshot_filename

        # Write with compression
        data.write_parquet(snapshot_path, compression="zstd")

        logger.success(
            f"Created snapshot: {snapshot_path} ({len(data):,} rows, "
            f"{snapshot_path.stat().st_size / 1024 / 1024:.2f} MB)"
        )

        return snapshot_path

    def restore_snapshot(self, snapshot_path: Path, target_dir: Path) -> None:
        """Restore snapshot by splitting into individual ticker files.

        Args:
            snapshot_path: Path to monolithic snapshot file
            target_dir: Directory where individual files will be written

        Raises:
            FileNotFoundError: If snapshot file doesn't exist
            ValueError: If snapshot contains no data or missing ticker column
        """
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Snapshot file not found: {snapshot_path}")

        logger.info(f"Restoring snapshot from {snapshot_path}")

        # Read snapshot
        snapshot_data = pl.read_parquet(snapshot_path)

        if snapshot_data.is_empty():
            raise ValueError("Snapshot contains no data")

        if "ticker" not in snapshot_data.columns:
            raise ValueError("Snapshot missing 'ticker' column")

        # Ensure target directory exists
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Get unique tickers
        tickers = snapshot_data.select("ticker").unique().to_series().to_list()
        logger.info(f"Restoring {len(tickers)} tickers to {target_dir}")

        # Iterate and write individual files
        for ticker in tickers:
            ticker_df = snapshot_data.filter(pl.col("ticker") == ticker)
            ticker_path = target_dir / f"{ticker}.parquet"

            ticker_df.write_parquet(ticker_path, compression="zstd")
            logger.debug(f"Restored {ticker}: {len(ticker_df)} rows -> {ticker_path}")

        logger.success(f"Restored {len(tickers)} ticker files to {target_dir}")

    def list_snapshots(self, data_type: str | None = None) -> list[Path]:
        """List available snapshots in archive directory.

        Args:
            data_type: Filter by data type ("prices" or "fundamentals"), or None for all

        Returns:
            List of snapshot file paths, sorted by date (newest first)
        """
        if data_type:
            pattern = f"{data_type}_snapshot_*.parquet"
        else:
            pattern = "*_snapshot_*.parquet"

        snapshots = sorted(self.archive_dir.glob(pattern), reverse=True)
        logger.debug(f"Found {len(snapshots)} snapshots matching '{pattern}'")

        return snapshots
