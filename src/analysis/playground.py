# src/analysis/playground.py (Vorschlag)
from pathlib import Path

import polars as pl

from src.analysis.metrics import MetricsEngine


def main() -> None:
    # Load Data (aus deinem Prod Ordner)
    base_dir = Path("data/prod")

    # Lese ALLE Prices (Wildcard trick)
    # Hinweis: Falls du noch einzelne Files hast, nutzen wir hier einen glob
    # Wenn wir WI-06 schon hätten, wäre es einfacher. So machen wir es manuell:
    try:
        q_prices = pl.scan_parquet(base_dir / "prices/*.parquet")
        q_fund = pl.scan_parquet(base_dir / "fundamentals/*.parquet")

        df_prices = q_prices.collect()
        df_fund = q_fund.collect()
    except Exception as e:
        print(f"Fehler beim Laden: {e}")
        return

    engine = MetricsEngine()
    test_tickers = ["MSFT", "GOOG", "MC.PA", "MUV2.DE"]

    df_fund_calc = engine.calculate_fundamental_metrics(df_fund)
    df_val = engine.calculate_valuation_metrics(df_prices, df_fund_calc)
    for ticker in test_tickers:
        # 1. Fundamentals
        print(f"--- Fundamentals (ROCE Example: {ticker}) ---")
        print(
            df_fund_calc.filter(pl.col("ticker") == ticker)
            .select(["report_date", "roce", "net_debt"])
            .tail(3)
        )

        # 2. Valuation
        print(f"\n--- Valuation (FCF Yield Example: {ticker}) ---")
        print(
            df_val.filter(pl.col("ticker") == ticker)
            .select(["date", "close", "dividend_yield", "fcf_yield"])
            .tail(5)
        )


if __name__ == "__main__":
    main()
