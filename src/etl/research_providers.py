"""Research script to compare data providers for financial data quality.

This script evaluates yfinance data coverage and quality metrics
to validate it as a reliable data source.
"""

import warnings
from datetime import datetime
from typing import Any, cast

import polars as pl
import yfinance as yf

# Test universe: mix of US and international stocks
TEST_TICKERS = [
    # US Stocks
    "MSFT",  # US: Microsoft
    "SPGI",  # US: S&P Global
    "GOOG",  # US: Alphabet (Google)
    "TMO",  # US: Thermo Fisher Scientific
    "V",  # US: Visa
    # European Stocks
    "MUV2.DE",  # Germany: Munich Re
    "SY1.DE",  # Germany: Symrise
    "UNA.AS",  # Netherlands: Unilever
    "ASML.AS",  # Netherlands: ASML
    "ROG.SW",  # Switzerland: Roche
    "NOVO-B.CO",  # Denmark: Novo Nordisk (B shares)
    "ATCO-A.ST",  # Sweden: Atlas Copco (A shares)
    "SU.PA",  # France: Schneider Electric
    "MC.PA",  # France: LVMH
    "AI.PA",  # France: Air Liquide
    # Asian Stocks
    "8001.T",  # Japan: Itochu Corporation
]

# Critical columns to check for data quality
BALANCE_SHEET_CRITICAL_COLS = [
    "total_assets",
    "total_liabilities_net_minority_interest",
    "stockholders_equity",
]

INCOME_STATEMENT_CRITICAL_COLS = [
    "total_revenue",
    "net_income",
    "ebitda",
]


def fetch_stock_info(ticker: str) -> dict[str, Any] | None:
    """Fetch current stock price and basic info using yfinance.

    Returns dict with price, currency, and exchange info or None if fetch fails.
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        # Get current price (try multiple fields as they vary)
        price = (
            info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
        )

        return {
            "price": price,
            "currency": info.get("currency", "USD"),
            "exchange": info.get("exchange", "N/A"),
            "company_name": info.get("shortName", ticker),
        }
    except Exception as e:
        warnings.warn(f"Failed to fetch info for {ticker}: {str(e)}", stacklevel=2)
        return None


def fetch_balance_sheet(ticker: str) -> pl.DataFrame | None:
    """Fetch balance sheet data using yfinance.

    Returns Polars DataFrame or None if fetch fails.
    """
    try:
        stock = yf.Ticker(ticker)
        df_pandas = stock.balance_sheet

        if df_pandas is not None and not df_pandas.empty:
            # Convert column names to lowercase snake_case
            df_pandas.index = df_pandas.index.str.lower().str.replace(" ", "_")
            # Transpose so dates are rows
            df_pandas = df_pandas.T
            return pl.from_pandas(df_pandas.reset_index())

        return None
    except Exception as e:
        warnings.warn(f"Failed to fetch balance sheet for {ticker}: {str(e)}", stacklevel=2)
        return None


def fetch_income_statement(ticker: str) -> pl.DataFrame | None:
    """Fetch income statement data using yfinance.

    Returns Polars DataFrame or None if fetch fails.
    """
    try:
        stock = yf.Ticker(ticker)
        df_pandas = stock.income_stmt

        if df_pandas is not None and not df_pandas.empty:
            # Convert column names to lowercase snake_case
            df_pandas.index = df_pandas.index.str.lower().str.replace(" ", "_")
            # Transpose so dates are rows
            df_pandas = df_pandas.T
            return pl.from_pandas(df_pandas.reset_index())

        return None
    except Exception as e:
        warnings.warn(f"Failed to fetch income statement for {ticker}: {str(e)}", stacklevel=2)
        return None


def calculate_quality_score(df: pl.DataFrame | None, critical_columns: list[str]) -> dict[str, Any]:
    """Calculate data quality metrics for a financial statement DataFrame.

    Metrics include row count, critical column coverage, and null value counts.
    """
    if df is None or df.height == 0:
        return {
            "rows_count": 0,
            "critical_cols_found": 0,
            "critical_cols_missing": len(critical_columns),
            "null_count": 0,
            "data_completeness_pct": 0.0,
        }

    # Normalize column names to lowercase for comparison
    df_cols_lower = {col.lower() for col in df.columns}
    critical_cols_lower = {col.lower() for col in critical_columns}

    # Check which critical columns are present
    cols_found = critical_cols_lower & df_cols_lower
    cols_missing = critical_cols_lower - df_cols_lower

    # Count nulls in critical columns that exist
    null_count = 0
    total_cells = 0

    for col in df.columns:
        if col.lower() in cols_found:
            null_count += df[col].null_count()
            total_cells += df.height

    # Calculate data completeness percentage
    completeness = ((total_cells - null_count) / total_cells * 100) if total_cells > 0 else 0.0

    return {
        "rows_count": df.height,
        "critical_cols_found": len(cols_found),
        "critical_cols_missing": len(cols_missing),
        "null_count": null_count,
        "data_completeness_pct": round(completeness, 2),
    }


def run_data_analysis() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Execute data quality analysis across all tickers.

    Returns tuple of (quality_results_df, price_results_df).
    """
    quality_results = []
    price_results = []

    for ticker in TEST_TICKERS:
        print(f"\n{'='*60}")
        print(f"Processing: {ticker}")
        print(f"{'='*60}")

        # Fetch current stock price and info
        stock_info = fetch_stock_info(ticker)
        if stock_info:
            price_results.append(
                {
                    "ticker": ticker,
                    "company_name": stock_info["company_name"],
                    "price": stock_info["price"],
                    "currency": stock_info["currency"],
                    "exchange": stock_info["exchange"],
                }
            )
            print(f"  Company: {stock_info['company_name']}")
            print(f"  Price: {stock_info['price']} {stock_info['currency']}")
        else:
            print("  Price: N/A")

        # Fetch Balance Sheet
        print("\n  Fetching Balance Sheet...")
        bs_df = fetch_balance_sheet(ticker)
        bs_quality = calculate_quality_score(bs_df, BALANCE_SHEET_CRITICAL_COLS)

        quality_results.append(
            {
                "ticker": ticker,
                "statement_type": "balance_sheet",
                **bs_quality,
            }
        )

        print(f"    ✓ {bs_quality['rows_count']} periods found")
        print(
            f"    ✓ {bs_quality['critical_cols_found']}/{len(BALANCE_SHEET_CRITICAL_COLS)} "
            f"critical columns present"
        )
        print(f"    ✓ {bs_quality['data_completeness_pct']}% data completeness")

        if bs_quality["critical_cols_missing"] > 0:
            print(f"    ⚠ {bs_quality['critical_cols_missing']} critical columns missing")

        # Fetch Income Statement
        print("\n  Fetching Income Statement...")
        is_df = fetch_income_statement(ticker)
        is_quality = calculate_quality_score(is_df, INCOME_STATEMENT_CRITICAL_COLS)

        quality_results.append(
            {
                "ticker": ticker,
                "statement_type": "income_statement",
                **is_quality,
            }
        )

        print(f"    ✓ {is_quality['rows_count']} periods found")
        print(
            f"    ✓ {is_quality['critical_cols_found']}/{len(INCOME_STATEMENT_CRITICAL_COLS)} "
            f"critical columns present"
        )
        print(f"    ✓ {is_quality['data_completeness_pct']}% data completeness")

        if is_quality["critical_cols_missing"] > 0:
            print(f"    ⚠ {is_quality['critical_cols_missing']} critical columns missing")

    return pl.DataFrame(quality_results), pl.DataFrame(price_results)


def generate_summary_report(results_df: pl.DataFrame, price_df: pl.DataFrame) -> None:
    """Generate and print summary comparison report."""
    print("\n" + "=" * 80)
    print("DATA QUALITY ANALYSIS REPORT - yfinance")
    print("=" * 80)

    # Price summary
    if price_df.height > 0:
        print("\n💰 CURRENT STOCK PRICES:")
        print(price_df)

    # Overall summary
    overall_summary = results_df.select(
        [
            pl.col("rows_count").sum().alias("total_periods"),
            pl.col("rows_count").mean().alias("avg_periods_per_statement"),
            pl.col("data_completeness_pct").mean().alias("avg_completeness_pct"),
            pl.col("critical_cols_missing").sum().alias("total_missing_cols"),
            pl.col("null_count").sum().alias("total_nulls"),
        ]
    )

    print("\n📊 OVERALL DATA QUALITY:")
    print(overall_summary)

    # Detailed breakdown by ticker
    print("\n\n📋 BREAKDOWN BY TICKER:")
    detailed = (
        results_df.group_by("ticker")
        .agg(
            [
                pl.col("rows_count").sum().alias("total_periods"),
                pl.col("data_completeness_pct").mean().alias("avg_completeness"),
                pl.col("critical_cols_missing").sum().alias("missing_critical_cols"),
            ]
        )
        .sort("ticker")
    )
    print(detailed)

    # Statement type performance
    print("\n\n📈 PERFORMANCE BY STATEMENT TYPE:")
    statement_perf = results_df.group_by("statement_type").agg(
        [
            pl.col("rows_count").mean().alias("avg_periods"),
            pl.col("data_completeness_pct").mean().alias("avg_completeness"),
            pl.col("critical_cols_missing").mean().alias("avg_missing_cols"),
        ]
    )
    print(statement_perf)

    # Quality assessment
    print("\n\n💡 QUALITY ASSESSMENT:")

    avg_completeness = overall_summary["avg_completeness_pct"][0]
    avg_periods = overall_summary["avg_periods_per_statement"][0]
    total_missing = overall_summary["total_missing_cols"][0]

    print(f"\n  Average Data Completeness: {avg_completeness:.1f}%")
    print(f"  Average Historical Periods: {avg_periods:.1f}")
    print(f"  Total Missing Critical Columns: {total_missing}")

    if avg_completeness >= 80 and avg_periods >= 3:
        print("\n✅ yfinance provides HIGH QUALITY data - suitable for production use")
    elif avg_completeness >= 60 and avg_periods >= 2:
        print("\n⚠️  yfinance provides ACCEPTABLE data - some gaps may exist")
    else:
        print("\n❌ yfinance data quality is LOW - consider alternative sources")

    # International coverage analysis
    international_stocks = results_df.filter(
        ~pl.col("ticker").str.contains("^[A-Z]+$")  # Exclude pure US tickers
    )
    us_stocks = results_df.filter(
        pl.col("ticker").str.contains("^[A-Z]+$")  # Only pure US tickers
    )

    if international_stocks.height > 0 and us_stocks.height > 0:
        intl_mean = international_stocks["data_completeness_pct"].mean()
        us_mean = us_stocks["data_completeness_pct"].mean()
        intl_avg_complete = cast(float, intl_mean) if intl_mean is not None else 0.0
        us_avg_complete = cast(float, us_mean) if us_mean is not None else 0.0
        print(f"\n🌍 International Coverage: {intl_avg_complete:.1f}% complete")
        print(f"🇺🇸 US Coverage: {us_avg_complete:.1f}% complete")


def main() -> None:
    """Main execution function."""
    print("🔬 Starting yfinance Data Quality Analysis...")
    print(f"Testing tickers: {', '.join(TEST_TICKERS)}")
    print(f"Analysis date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Run analysis
    quality_results, price_results = run_data_analysis()

    # Generate report
    generate_summary_report(quality_results, price_results)

    print("\n" + "=" * 80)
    print("✅ Analysis Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
