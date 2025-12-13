"""ETL Pipeline orchestration with gap detection and atomic writes.

Coordinates extraction, transformation, and loading of stock market data.
Implements incremental load logic for prices and full refresh for fundamentals.
"""

from datetime import date, timedelta

import polars as pl
from loguru import logger

from src.core.domain_models import ReportType
from src.core.file_manager import ParquetStorage
from src.core.mapper import map_fundamentals_to_domain, map_prices_to_df
from src.etl.extract import DataExtractor


class ETLPipeline:
    """Orchestrates ETL operations with dependency injection for testability."""

    # Hard constraint for initial price data fetch
    INITIAL_START_DATE = date(2021, 1, 1)

    def __init__(self, storage: ParquetStorage, extractor: DataExtractor) -> None:
        """
        Initialize pipeline with storage and extraction dependencies.

        Args:
            storage: ParquetStorage instance for reading/writing data
            extractor: DataExtractor instance for fetching from yfinance
        """
        self.storage = storage
        self.extractor = extractor
        logger.info("ETLPipeline initialized")

    def run_price_update(self, tickers: list[str]) -> None:
        """
        Update price data for multiple tickers with gap detection.

        Implements incremental load strategy:
        - If data exists: Fetch only from last known date + 1 day
        - If new ticker: Fetch from 2021-01-01 (hard constraint)

        Merges new data with existing data, removes duplicates, and writes atomically.

        Args:
            tickers: List of stock ticker symbols to update
        """
        logger.info(f"Starting price update for {len(tickers)} tickers")

        for ticker in tickers:
            try:
                # Gap Detection: Check if we already have data for this ticker
                filename = f"prices_{ticker}"
                start_date = self._detect_price_gap(filename)

                # Fetch new data from yfinance
                raw_pdf = self.extractor.get_prices(ticker, start_date)
                currency = self.extractor.get_ticker_info(ticker).get("currency", "USD")

                # Transform to domain model
                new_df = map_prices_to_df(raw_pdf, ticker, currency)

                # Merge with existing data if available
                merged_df = self._merge_price_data(filename, new_df)

                # Atomic write back to storage
                self.storage.atomic_write(merged_df, filename)

                logger.success(f"[{ticker}] Price update complete ({len(merged_df)} total rows)")

            except ValueError as e:
                # No data found - log warning but continue with other tickers
                logger.warning(f"[{ticker}] Skipped: {e}")
                continue

            except Exception as e:
                # Unexpected error - log but don't crash entire pipeline
                logger.error(f"[{ticker}] Price update failed: {e}")
                continue

    def run_fundamental_update(self, tickers: list[str]) -> None:
        """
        Update fundamental data for multiple tickers using full refresh strategy.

        Always fetches complete financial history and overwrites existing data.
        This is appropriate for fundamentals since they change infrequently
        and the data volume is manageable.

        Args:
            tickers: List of stock ticker symbols to update
        """
        logger.info(f"Starting fundamental update for {len(tickers)} tickers")

        for ticker in tickers:
            try:
                # Fetch complete financial statements from yfinance
                raw_pdf = self.extractor.get_financials(ticker)
                currency = self.extractor.get_ticker_info(ticker).get("currency", "USD")

                # Transform to domain model (list of FinancialReport objects)
                reports = map_fundamentals_to_domain(raw_pdf, ticker, ReportType.ANNUAL, currency)

                if not reports:
                    logger.warning(f"[{ticker}] Mapping produced no reports")
                    continue

                # Convert to Polars DataFrame for storage
                records = [report.model_dump() for report in reports]
                fundamentals_df = pl.DataFrame(records)

                # Atomic overwrite of existing data
                filename = f"fundamentals_{ticker}"
                self.storage.atomic_write(fundamentals_df, filename)

                logger.success(f"[{ticker}] Fundamental update complete ({len(reports)} reports)")

            except ValueError as e:
                # No data found - log warning but continue with other tickers
                logger.warning(f"[{ticker}] Skipped: {e}")
                continue

            except Exception as e:
                # Unexpected error - log but don't crash entire pipeline
                logger.error(f"[{ticker}] Fundamental update failed: {e}")
                continue

    def _detect_price_gap(self, filename: str) -> date:
        """
        Determine the start date for incremental price data fetch.

        Logic:
        - If file exists: Return max(date) + 1 day (fetch only new data)
        - If file doesn't exist: Return INITIAL_START_DATE (full history)

        Args:
            filename: Parquet filename (without .parquet extension)

        Returns:
            Date to start fetching from
        """
        try:
            existing_df = self.storage.read(filename)

            # Find the most recent date in existing data
            max_date_value = existing_df.select(pl.col("date").max()).item()
            assert isinstance(max_date_value, date), "Expected date type from max(date)"

            # Start from the next day
            start_date = max_date_value + timedelta(days=1)

            logger.info(f"[{filename}] Gap detected: fetching from {start_date}")
            return start_date

        except FileNotFoundError:
            # No existing data - start from hard constraint date
            logger.info(f"[{filename}] New ticker: fetching from {self.INITIAL_START_DATE}")
            return self.INITIAL_START_DATE

    def _merge_price_data(self, filename: str, new_df: pl.DataFrame) -> pl.DataFrame:
        """
        Merge new price data with existing data, removing duplicates.

        Strategy:
        - If file exists: vstack old + new, deduplicate by date, sort
        - If file doesn't exist: return new data as-is

        Args:
            filename: Parquet filename (without .parquet extension)
            new_df: New price data to merge

        Returns:
            Merged and deduplicated DataFrame
        """
        try:
            existing_df = self.storage.read(filename)

            # Stack old and new data vertically
            merged = (
                pl.concat([existing_df, new_df])
                .unique(subset=["date"], maintain_order=False)
                .sort("date")
            )

            logger.debug(
                f"[{filename}] Merged {len(existing_df)} + {len(new_df)} "
                f"→ {len(merged)} rows (after dedup)"
            )
            return merged

        except FileNotFoundError:
            # No existing data - just return new data
            logger.debug(f"[{filename}] No existing data, using new data as-is")
            return new_df
