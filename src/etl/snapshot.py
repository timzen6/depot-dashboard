from loguru import logger

from src.config.settings import load_config
from src.data_mgmt.archiver import DataArchiver


def make_snapshot(data_type: str | None = None) -> None:
    """Create compressed snapshots of price and fundamental data."""
    logger.info("=== Creating Data Snapshots ===")

    # Load configuration
    config = load_config()

    # Initialize archiver
    archiver = DataArchiver(config.settings.base_dir, config.settings.archive_dir)

    # Create snapshots for all data types
    if data_type:
        data_types = [data_type]
    else:
        data_types = [
            "metadata",
            "prices",
            "fundamentals/annual",
            "fundamentals/quarterly",
        ]

    for data_type in data_types:
        try:
            snapshot_path = archiver.create_snapshot(data_type)
            logger.success(f"Created {data_type} snapshot: {snapshot_path}")
        except Exception as e:
            logger.error(f"Failed to create {data_type} snapshot: {e}")
