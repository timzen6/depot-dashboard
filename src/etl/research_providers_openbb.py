"""Research script to evaluate OpenBB data providers for financial data quality.

This script tests OpenBB SDK with multiple providers (yfinance, fmp, etc.)
to compare data coverage and quality metrics.

NOTE: As of December 2025, OpenBB SDK has compatibility issues with internal
imports (OBBject_EquityInfo missing). This script is provided for reference
but may not work until OpenBB fixes their package dependencies.

RECOMMENDATION: Use research_providers.py (yfinance directly) instead.
"""

import warnings
from datetime import datetime
from typing import Any

import polars as pl

try:
    from openbb import obb

    OPENBB_AVAILABLE = True
except ImportError:
    OPENBB_AVAILABLE = False
    warnings.warn("OpenBB is not installed. Install with: pip install openbb", stacklevel=2)


# Test universe: mix of US and international stocks
TEST_TICKERS = [
    "MSFT",  # US: Microsoft
    "AAPL",  # US: Apple
    "MUV2.DE",  # Germany: Munich Re
    "UNA.AS",  # Netherlands: Unilever
    "MC.PA",  # France: LVMH
]

# Providers to test (fmp requires API key)
PROVIDERS = ["yfinance", "fmp"]

# Critical columns to check for data quality
BALANCE_SHEET_CRITICAL_COLS = [
    "total_assets",
    "total_liabilities",
    "total_equity",
]

INCOME_STATEMENT_CRITICAL_COLS = [
    "revenue",
    "net_income",
    "ebitda",
]


def fetch_stock_quote(ticker: str, provider: str) -> dict[str, Any] | None:
    """Fetch current stock quote using OpenBB.

    Returns dict with price and basic info or None if fetch fails.
    """
    if not OPENBB_AVAILABLE:
        return None

    try:
        result = obb.equity.price.quote(symbol=ticker, provider=provider)

        if result and hasattr(result, "results"):
            data = result.results[0] if isinstance(result.results, list) else result.results

            # Extract key fields (field names vary by provider)
            price = (
                getattr(data, "last_price", None)
                or getattr(data, "price", None)
                or getattr(data, "previous_close", None)
            )

            return {
                "price": price,
                "symbol": getattr(data, "symbol", ticker),
                "provider": provider,
            }
        return None
    except Exception as e:
        warnings.warn(f"Failed to fetch quote for {ticker} via {provider}: {str(e)}", stacklevel=2)
        return None


def fetch_balance_sheet_openbb(ticker: str, provider: str) -> pl.DataFrame | None:
    """Fetch balance sheet data using OpenBB.

    Returns Polars DataFrame or None if fetch fails.
    """
    if not OPENBB_AVAILABLE:
        return None

    try:
        result = obb.equity.fundamental.balance(symbol=ticker, provider=provider, limit=10)

        if result:
            # Try to convert to Polars
            if hasattr(result, "to_polars"):
                df: pl.DataFrame = result.to_polars()
                return df
            elif hasattr(result, "to_df"):
                df_pandas = result.to_df()
                if df_pandas is not None and not df_pandas.empty:
                    return pl.from_pandas(df_pandas)

        return None
    except Exception as e:
        warnings.warn(
            f"Failed to fetch balance sheet for {ticker} via {provider}: {str(e)}",
            stacklevel=2,
        )
        return None


def fetch_income_statement_openbb(ticker: str, provider: str) -> pl.DataFrame | None:
    """Fetch income statement data using OpenBB.

    Returns Polars DataFrame or None if fetch fails.
    """
    if not OPENBB_AVAILABLE:
        return None

    try:
        result = obb.equity.fundamental.income(symbol=ticker, provider=provider, limit=10)

        if result:
            # Try to convert to Polars
            if hasattr(result, "to_polars"):
                df: pl.DataFrame = result.to_polars()
                return df
            elif hasattr(result, "to_df"):
                df_pandas = result.to_df()
                if df_pandas is not None and not df_pandas.empty:
                    return pl.from_pandas(df_pandas)

        return None
    except Exception as e:
        warnings.warn(
            f"Failed to fetch income statement for {ticker} via {provider}: {str(e)}",
            stacklevel=2,
        )
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


def run_provider_comparison() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Execute provider comparison across all tickers.

    Returns tuple of (quality_results_df, price_results_df).
    """
    if not OPENBB_AVAILABLE:
        print("❌ OpenBB is not available. Cannot run comparison.")
        return pl.DataFrame(), pl.DataFrame()

    quality_results = []
    price_results = []

    for ticker in TEST_TICKERS:
        print(f"\n{'='*60}")
        print(f"Processing: {ticker}")
        print(f"{'='*60}")

        for provider in PROVIDERS:
            print(f"\n  Provider: {provider}")

            # Fetch current quote
            quote_data = fetch_stock_quote(ticker, provider)
            if quote_data:
                price_results.append(
                    {
                        "ticker": ticker,
                        "provider": provider,
                        "price": quote_data["price"],
                    }
                )
                print(f"    Current Price: {quote_data['price']}")
            else:
                print("    Current Price: N/A")

            # Fetch Balance Sheet
            print("    Fetching Balance Sheet...")
            bs_df = fetch_balance_sheet_openbb(ticker, provider)
            bs_quality = calculate_quality_score(bs_df, BALANCE_SHEET_CRITICAL_COLS)

            quality_results.append(
                {
                    "ticker": ticker,
                    "provider": provider,
                    "statement_type": "balance_sheet",
                    **bs_quality,
                }
            )

            print(
                f"      ✓ {bs_quality['rows_count']} periods | "
                f"{bs_quality['critical_cols_found']}/{len(BALANCE_SHEET_CRITICAL_COLS)} cols | "
                f"{bs_quality['data_completeness_pct']}% complete"
            )

            # Fetch Income Statement
            print("    Fetching Income Statement...")
            is_df = fetch_income_statement_openbb(ticker, provider)
            is_quality = calculate_quality_score(is_df, INCOME_STATEMENT_CRITICAL_COLS)

            quality_results.append(
                {
                    "ticker": ticker,
                    "provider": provider,
                    "statement_type": "income_statement",
                    **is_quality,
                }
            )

            print(
                f"      ✓ {is_quality['rows_count']} periods | "
                f"{is_quality['critical_cols_found']}/{len(INCOME_STATEMENT_CRITICAL_COLS)} cols | "
                f"{is_quality['data_completeness_pct']}% complete"
            )

    return pl.DataFrame(quality_results), pl.DataFrame(price_results)


def generate_summary_report(results_df: pl.DataFrame, price_df: pl.DataFrame) -> None:
    """Generate and print summary comparison report."""
    print("\n" + "=" * 80)
    print("OPENBB PROVIDER COMPARISON REPORT")
    print("=" * 80)

    if results_df.height == 0:
        print("\n⚠️  No data collected. Check OpenBB installation and API keys.")
        return

    # Price comparison
    if price_df.height > 0:
        print("\n💰 PRICE DATA AVAILABILITY:")
        price_summary = price_df.group_by("provider").agg(
            [
                pl.col("ticker").count().alias("quotes_fetched"),
                pl.col("price").is_not_null().sum().alias("prices_available"),
            ]
        )
        print(price_summary)

    # Provider comparison
    print("\n📊 PROVIDER DATA QUALITY COMPARISON:")
    provider_summary = (
        results_df.group_by("provider")
        .agg(
            [
                pl.col("rows_count").sum().alias("total_periods"),
                pl.col("rows_count").mean().alias("avg_periods_per_statement"),
                pl.col("data_completeness_pct").mean().alias("avg_completeness_pct"),
                pl.col("critical_cols_missing").sum().alias("total_missing_cols"),
                pl.col("null_count").sum().alias("total_nulls"),
            ]
        )
        .sort("avg_completeness_pct", descending=True)
    )
    print(provider_summary)

    # Detailed breakdown by ticker and provider
    print("\n\n📋 BREAKDOWN BY TICKER & PROVIDER:")
    detailed = (
        results_df.group_by(["ticker", "provider"])
        .agg(
            [
                pl.col("rows_count").sum().alias("total_periods"),
                pl.col("data_completeness_pct").mean().alias("avg_completeness"),
                pl.col("critical_cols_missing").sum().alias("missing_cols"),
            ]
        )
        .sort(["ticker", "provider"])
    )
    print(detailed)

    # Statement type performance by provider
    print("\n\n📈 PERFORMANCE BY STATEMENT TYPE & PROVIDER:")
    statement_perf = (
        results_df.group_by(["provider", "statement_type"])
        .agg(
            [
                pl.col("rows_count").mean().alias("avg_periods"),
                pl.col("data_completeness_pct").mean().alias("avg_completeness"),
            ]
        )
        .sort(["statement_type", "provider"])
    )
    print(statement_perf)

    # International vs US coverage by provider
    print("\n\n🌍 INTERNATIONAL vs US COVERAGE:")

    intl_results = results_df.filter(~pl.col("ticker").str.contains("^[A-Z]+$"))
    us_results = results_df.filter(pl.col("ticker").str.contains("^[A-Z]+$"))

    if intl_results.height > 0 and us_results.height > 0:
        coverage_comparison = pl.concat(
            [
                intl_results.group_by("provider").agg(
                    [
                        pl.lit("International").alias("market"),
                        pl.col("data_completeness_pct").mean().alias("avg_completeness"),
                        pl.col("rows_count").mean().alias("avg_periods"),
                    ]
                ),
                us_results.group_by("provider").agg(
                    [
                        pl.lit("US").alias("market"),
                        pl.col("data_completeness_pct").mean().alias("avg_completeness"),
                        pl.col("rows_count").mean().alias("avg_periods"),
                    ]
                ),
            ]
        ).sort(["market", "provider"])

        print(coverage_comparison)

    # Recommendation
    print("\n\n💡 RECOMMENDATION:")

    if provider_summary.height >= 2:
        # Compare top providers
        top_provider = provider_summary[0]
        second_provider = provider_summary[1] if provider_summary.height > 1 else None

        print(f"\n  🥇 Best: {top_provider['provider'][0]}")
        print(f"     - Completeness: {top_provider['avg_completeness_pct'][0]:.1f}%")
        print(f"     - Avg Periods: {top_provider['avg_periods_per_statement'][0]:.1f}")
        print(f"     - Missing Columns: {top_provider['total_missing_cols'][0]}")

        if second_provider is not None:
            print(f"\n  🥈 Alternative: {second_provider['provider'][0]}")
            print(f"     - Completeness: {second_provider['avg_completeness_pct'][0]:.1f}%")
            print(f"     - Avg Periods: {second_provider['avg_periods_per_statement'][0]:.1f}")
            print(f"     - Missing Columns: {second_provider['total_missing_cols'][0]}")

            # Check if FMP is significantly better
            if (
                top_provider["provider"][0] == "fmp"
                and top_provider["avg_completeness_pct"][0]
                > second_provider["avg_completeness_pct"][0] + 10
            ):
                print(
                    "\n  ✅ FMP provides significantly better data - "
                    "worth considering paid subscription"
                )
            elif top_provider["provider"][0] == "yfinance":
                print("\n  ✅ yfinance is sufficient - no need for paid FMP subscription")
            else:
                print("\n  ⚖️  Comparable quality - choose based on specific needs and budget")
    elif provider_summary.height == 1:
        single_provider = provider_summary[0]
        print(f"\n  Only {single_provider['provider'][0]} returned data")
        print(f"  Completeness: {single_provider['avg_completeness_pct'][0]:.1f}%")


def main() -> None:
    """Main execution function."""
    if not OPENBB_AVAILABLE:
        print("❌ OpenBB SDK is not installed.")
        print("\nTo install OpenBB:")
        print("  pip install openbb")
        print("\nOr with specific providers:")
        print("  pip install 'openbb[all]'")
        return

    print("🔬 Starting OpenBB Provider Comparison Analysis...")
    print(f"Testing tickers: {', '.join(TEST_TICKERS)}")
    print(f"Providers: {', '.join(PROVIDERS)}")
    print(f"Analysis date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    print("⚠️  Note: FMP provider requires API key (set FMP_API_KEY env variable)")
    print("    If not set, FMP tests will fail - this is expected.\n")

    # Run comparison
    quality_results, price_results = run_provider_comparison()

    # Generate report
    generate_summary_report(quality_results, price_results)

    print("\n" + "=" * 80)
    print("✅ Analysis Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
