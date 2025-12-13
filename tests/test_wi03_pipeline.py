"""
WI-03 ETL Pipeline Demo & Validation

Demonstrates the complete ETL pipeline with:
- Gap detection and incremental price updates
- Full refresh fundamental updates
- Robust error handling with retries
- Atomic writes for data integrity
"""

from pathlib import Path

from loguru import logger

from src.core.file_manager import ParquetStorage
from src.etl.extract import DataExtractor
from src.etl.pipeline import ETLPipeline

# Test tickers (mix of US and European stocks)
TEST_TICKERS = [
    "MSFT",
    "GOOG",
    "V",
    "ATCO-A.ST",
    "SU.PA",
    "MUV2.DE",
    "EURUSD=X",  # Standard Forex pair
    "BTC-EUR",  # Crypto behaving like FX
]
NO_FUNDAMENTALS_TICKERS = [
    "EURUSD=X",  # Standard Forex pair
    "BTC-EUR",  # Crypto behaving like FX
]


def main() -> None:
    """Run WI-03 ETL Pipeline demonstration."""
    logger.info("=== WI-03 ETL Pipeline Demo ===")

    # Setup storage directories
    base_dir = Path("data/test_wi03")
    prices_dir = base_dir / "prices"
    fundamentals_dir = base_dir / "fundamentals"

    prices_dir.mkdir(parents=True, exist_ok=True)
    fundamentals_dir.mkdir(parents=True, exist_ok=True)

    # Initialize components via dependency injection
    prices_storage = ParquetStorage(prices_dir)
    fundamentals_storage = ParquetStorage(fundamentals_dir)
    extractor = DataExtractor()

    # Create pipeline instances
    price_pipeline = ETLPipeline(prices_storage, extractor)
    fundamental_pipeline = ETLPipeline(fundamentals_storage, extractor)

    # Test 1: Initial price load (should fetch from 2021-01-01)
    logger.info("\n--- Test 1: Initial Price Load ---")
    price_pipeline.run_price_update(TEST_TICKERS)

    # Test 2: Incremental price update (should detect gap and fetch only new data)
    logger.info("\n--- Test 2: Incremental Price Update (Gap Detection) ---")
    price_pipeline.run_price_update(TEST_TICKERS)

    # Test 3: Fundamental data load (full refresh)
    logger.info("\n--- Test 3: Fundamental Data Load ---")
    fundamental_pipeline.run_fundamental_update(TEST_TICKERS)

    # Test 4: Verify data integrity
    logger.info("\n--- Test 4: Data Integrity Check ---")
    for ticker in TEST_TICKERS:
        # A. Check Prices (must be available for ALL)
        try:
            prices_df = prices_storage.read(f"prices_{ticker}")
            logger.success(f"[{ticker}] ✓ Prices: {len(prices_df)} rows")
        except Exception as e:
            logger.error(f"[{ticker}] ✗ Prices check failed: {e}")

        # B. Check Fundamentals (only for stocks, skip for Forex/Crypto)
        try:
            fundamentals_df = fundamentals_storage.read(f"fundamentals_{ticker}")
            logger.success(f"[{ticker}] ✓ Fundamentals: {len(fundamentals_df)} reports")
        except Exception as e:
            # Check: Was the failure expected?
            if ticker in NO_FUNDAMENTALS_TICKERS:
                logger.info(f"[{ticker}] - No Fundamentals (Expected behavior)")
            else:
                # Real error: A stock should have fundamentals!
                logger.error(f"[{ticker}] ✗ Fundamentals check failed: {e}")

    logger.info("\n=== WI-03 Demo Complete ===")
    logger.info(f"Test data stored in: {base_dir.absolute()}")


if __name__ == "__main__":
    main()
