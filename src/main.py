import sys
from pathlib import Path

from loguru import logger

from src.core.file_manager import ParquetStorage
from src.etl.extract import DataExtractor
from src.etl.pipeline import ETLPipeline

# --- CONFIGURATION ---

# 1. Assets with fundamental data (stocks)
STOCKS = [
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

# 2. Assets with price data only (currencies, crypto, ETFs, indices)
ASSETS_PRICE_ONLY = [
    # -- Currencies & Crypto --
    "EURUSD=X",
    "CHFEUR=X",
    "DKKEUR=X",
    "SEKEUR=X",
    "JPYEUR=X",
    "BTC-EUR",
    # -- ETFs (via Xetra in EUR) --
    "EUNL.DE",  # iShares Core MSCI World
    # "WSRI.DE",  # Amundi MSCI World SRI
    "M7U.DE",
    "EUNM.DE",  # iShares MSCI EM (Acc)
    # "AEEM.DE",  # Amundi MSCI EM
    "AEME.PA",  # Amundi MSCI EM
    "EXSB.DE",  # iShares STOXX Europe Small 200
]

DATA_DIR = Path("data/prod")


def main() -> None:
    logger.info("🚀 Starting ETL Pipeline...")

    # Setup Infrastructure
    prices_dir = DATA_DIR / "prices"
    fund_dir = DATA_DIR / "fundamentals"

    prices_dir.mkdir(parents=True, exist_ok=True)
    fund_dir.mkdir(parents=True, exist_ok=True)

    # Init Components
    storage_prices = ParquetStorage(prices_dir)
    storage_fund = ParquetStorage(fund_dir)
    extractor = DataExtractor()

    pipeline_prices = ETLPipeline(storage_prices, extractor)
    pipeline_fund = ETLPipeline(storage_fund, extractor)

    try:
        # A. Prices Update (ALL assets need prices)
        # We combine both lists
        full_universe = STOCKS + ASSETS_PRICE_ONLY
        logger.info(f"--- Updating Prices for {len(full_universe)} assets ---")
        pipeline_prices.run_price_update(full_universe)

        # B. Fundamentals Update (ONLY stocks)
        # We skip calls for currencies entirely
        logger.info(f"--- Updating Fundamentals for {len(STOCKS)} stocks ---")
        pipeline_fund.run_fundamental_update(STOCKS)

        logger.success("✅ ETL Pipeline finished successfully.")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
