"""Portfolio Engine Testing Script.

Validates portfolio calculations and demonstrates engine usage.
Run after ETL pipeline has populated price data.
"""

import traceback

import polars as pl
from loguru import logger

from src.analysis.portfolio import PortfolioEngine
from src.config.settings import load_config


def main() -> None:
    """Test portfolio calculations for all defined portfolios."""
    try:
        config = load_config()
        num_portfolios = len(config.portfolios.portfolios) if config.portfolios else 0
        logger.info(f"Loaded config with {num_portfolios} portfolios")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return

    prices_path = config.settings.prices_dir
    if not prices_path.exists():
        logger.error(f"Price data not found at {prices_path}. Run ETL first.")
        return

    logger.info("Loading price data...")
    df_prices = pl.scan_parquet(prices_path / "*.parquet").collect()

    engine = PortfolioEngine()

    if not config.portfolios:
        logger.error("No portfolios configured")
        return

    for portfolio in config.portfolios.portfolios.values():
        print(f"\n{'='*60}")
        print(f"Portfolio: {portfolio.name} (Type: {portfolio.type.value})")
        print(f"{'='*60}")

        try:
            df_result = engine.calculate_portfolio_history(portfolio, df_prices)

            if df_result.is_empty():
                logger.warning(f"No results for {portfolio.name}")
                continue

            df_result = df_result.sort("date")

            print(f"Date range: {df_result['date'].min()!s} to " f"{df_result['date'].max()!s}")
            print("\n--- First 5 Records ---")
            print(df_result.head(5))
            print("\n--- Last 5 Records ---")
            print(df_result.tail(5))

            if "position_value" in df_result.columns:
                df_total = engine.aggregate_total_value(df_result)
                if not df_total.is_empty():
                    start_val = df_total["total_value"][0]
                    end_val = df_total["total_value"][-1]
                    perf = ((end_val - start_val) / start_val) * 100
                    print(f"\n📈 Performance: {start_val:.2f} → {end_val:.2f} ({perf:+.2f}%)")

        except Exception as e:
            logger.error(f"Error calculating {portfolio.name}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    main()
