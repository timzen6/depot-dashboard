import shutil
from pathlib import Path

import pandas as pd
import polars as pl
import yfinance as yf
from loguru import logger

from src.core.domain_models import STOCK_PRICE_SCHEMA, ReportType
from src.core.file_manager import ParquetStorage
from src.core.mapper import map_fundamentals_to_domain, map_prices_to_df

TEST_TICKERS = [
    # US Stocks
    "MSFT",
    "SPGI",
    "GOOG",
    "TMO",
    "V",
    # European Stocks
    "MUV2.DE",
    "SY1.DE",
    "UNA.AS",
    "ASML.AS",
    "ROG.SW",
    "NOVO-B.CO",
    "ATCO-A.ST",
    "SU.PA",
    "MC.PA",
    "AI.PA",
    # Asian Stocks
    "8001.T",
]


def setup() -> tuple[Path, Path]:
    """Clean setup environment and return storage directories."""
    test_dir = Path("data/test_wi02")

    if test_dir.exists():
        shutil.rmtree(test_dir)

    prices_dir = test_dir / "prices"
    fundamentals_dir = test_dir / "fundamentals"

    prices_dir.mkdir(parents=True, exist_ok=True)
    fundamentals_dir.mkdir(parents=True, exist_ok=True)

    return prices_dir, fundamentals_dir


def try_fetch_currency(ticker: str) -> str:
    """Attempt to fetch the currency for a ticker, defaulting to USD on failure."""
    yf_ticker = yf.Ticker(ticker)
    try:
        currency = str(yf_ticker.info.get("currency", "USD"))
        logger.info(f"[{ticker}] Detected currency: {currency}")
    except Exception:
        currency = "USD"
        logger.warning(f"[{ticker}] Could not fetch currency info, defaulting to USD.")
    return currency


def fetch_and_test_prices(storage: ParquetStorage) -> None:
    logger.info("--- Testing Prices (High Frequency) ---")

    for ticker in TEST_TICKERS:
        try:
            # 1. Fetch (1 month is sufficient for testing, faster than full history)
            raw_pdf = yf.download(
                ticker,
                period="1mo",
                progress=False,
                # This ensures that yfinance adjusts prices for splits/dividends
                # Needs to be set also in the future
                auto_adjust=True,
            )

            if raw_pdf.empty:
                logger.warning(f"[{ticker}] No price data found.")
                continue

            # 2. Map
            currency = try_fetch_currency(ticker)
            df_pl = map_prices_to_df(raw_pdf, ticker, currency)

            # 3. Validate Schema
            assert df_pl.schema == pl.Schema(STOCK_PRICE_SCHEMA)
            assert not df_pl.is_empty()

            # 4. Store
            filename = f"prices_{ticker}"
            storage.atomic_write(df_pl, filename)

            # 5. Read Back check
            df_read = storage.read(filename)
            assert df_read.height == df_pl.height

            logger.success(f"[{ticker}] Prices OK ({df_pl.height} rows)")

        except Exception as e:
            logger.error(f"[{ticker}] Price Test Failed: {e}")


def fetch_and_test_fundamentals(storage: ParquetStorage) -> None:
    logger.info("\n--- Testing Fundamentals (Complex Domain Models) ---")

    for ticker in TEST_TICKERS:
        try:
            yf_ticker = yf.Ticker(ticker)
            ticker_currency = try_fetch_currency(ticker)

            # 1. Fetch Annual Statements
            # yfinance returns 3 separate DataFrames (Income, Balance Sheet, Cash Flow)
            # Merge them for mapper since KPIs like ROCE require data from multiple statements
            inc = yf_ticker.financials
            bal = yf_ticker.balance_sheet
            cash = yf_ticker.cashflow

            if inc.empty and bal.empty:
                logger.warning(f"[{ticker}] No fundamental data found.")
                continue

            # Merge vertically (rows are metrics),
            # remove duplicates (e.g., Net Income appears in multiple statements)
            raw_combined = pd.concat([inc, bal, cash])
            raw_combined = raw_combined[~raw_combined.index.duplicated(keep="first")]

            # 2. Map
            reports = map_fundamentals_to_domain(
                raw_combined,
                ticker,
                ReportType.ANNUAL,
                currency=ticker_currency,
            )

            if not reports:
                logger.warning(f"[{ticker}] Mapping produced 0 reports (Check Key-Mapping?)")
                continue

            # 3. Validate Logic (Spot Check)
            latest_report = reports[0]  # Usually sorted by date desc by yfinance logic

            # Check Critical Fields for ROCE/FCF
            # Warn if None, but don't fail (Data might actually be missing)
            missing_fields = []
            if latest_report.ebit is None:
                missing_fields.append("EBIT")
            if latest_report.total_assets is None:
                missing_fields.append("Assets")

            log_method = logger.warning if missing_fields else logger.success
            log_method(
                f"[{ticker}] Fundamentals OK. Periods: {len(reports)}. "
                f"Missing: {missing_fields}"
            )

            # 4. Convert to Polars DataFrame and store
            records = [report.model_dump() for report in reports]
            df_pl = pl.DataFrame(records)

            filename = f"fundamentals_{ticker}"
            storage.atomic_write(df_pl, filename)

            # 5. Read back check
            df_read = storage.read(filename)
            assert df_read.height == len(reports)

            logger.success(f"[{ticker}] Stored {len(reports)} fundamental reports")

        except Exception as e:
            logger.error(f"[{ticker}] Fundamental Test Failed: {e}")


def main() -> None:
    prices_dir, fundamentals_dir = setup()

    prices_storage = ParquetStorage(prices_dir)
    fundamentals_storage = ParquetStorage(fundamentals_dir)

    fetch_and_test_prices(prices_storage)
    fetch_and_test_fundamentals(fundamentals_storage)

    logger.info("\nTest Artifacts stored:")
    logger.info(f"  - Prices: {prices_dir.absolute()}")
    logger.info(f"  - Fundamentals: {fundamentals_dir.absolute()}")


if __name__ == "__main__":
    main()
