"""Parquet file storage with atomic write guarantees.

Handles all file I/O operations using Polars DataFrames.
Atomic writes prevent data corruption by writing to temporary files first.
"""

from pathlib import Path

import polars as pl
from loguru import logger


class ParquetStorage:
    """Manages atomic read/write operations for Parquet files."""

    def __init__(self, base_path: Path) -> None:
        """Initialize storage with a base directory.

        Args:
            base_path: Root directory for all parquet files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"ParquetStorage initialized at {self.base_path}")

    def atomic_write(self, df: pl.DataFrame, filename: str) -> None:
        """Write DataFrame to parquet with atomic guarantees.

        Writes to a temporary file first, then renames to the target filename.
        This ensures the target file is never left in a partially written state.
        """
        if not filename.endswith(".parquet"):
            filename += ".parquet"

        target_path = self.base_path / filename
        tmp_path = self.base_path / f"{filename}.tmp"

        try:
            # Write to temporary file
            df.write_parquet(tmp_path)
            logger.debug(f"Wrote temporary file: {tmp_path}")

            # Atomic rename (overwrites target if it exists)
            tmp_path.replace(target_path)
            logger.info(f"Atomically wrote {len(df)} rows to {target_path}")

        except Exception as e:
            # Clean up temporary file on failure
            if tmp_path.exists():
                tmp_path.unlink()
            logger.error(f"Failed to write {filename}: {e}")
            raise

    def read(self, filename: str) -> pl.DataFrame:
        """Read parquet file into a Polars DataFrame."""
        target_path = self.base_path / f"{filename}.parquet"

        if not target_path.exists():
            logger.warning(f"File not found: {target_path}")
            raise FileNotFoundError(f"No parquet file found: {filename}")

        logger.debug(f"Reading {target_path}")
        data = pl.read_parquet(target_path)
        logger.info(f"Read {len(data)} rows from {filename}.parquet")

        return data
