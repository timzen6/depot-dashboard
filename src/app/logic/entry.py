import polars as pl
from dateutil.relativedelta import relativedelta

TRADING_DAYS_PER_YEAR = 252


def calculate_volatility_metrics(
    df_prices: pl.DataFrame,
    window_days: int,
    selected_tickers: list[str],
) -> pl.DataFrame:
    """Calculate volatility-related metrics for entry analysis."""
    selected_price_data = (
        df_prices.filter(pl.col("ticker").is_in(selected_tickers))
        .sort(["ticker", "date"])
        # 1. Basics
        .with_columns(
            [
                # tactical indicators for Z-Score and entry limits
                pl.col("close").rolling_mean(50).over("ticker").alias("sma_50"),
                pl.col("close").rolling_std(50).over("ticker").alias("std_50"),
                # structural info to understand the stock and its business
                pl.col("close").rolling_mean(200).over("ticker").alias("sma_200"),
                pl.col("close").rolling_std(200).over("ticker").alias("std_200"),
                pl.col("close").pct_change().over("ticker").alias("daily_return"),
            ]
        )
        # 2. Derived Metrics
        .with_columns(
            # Tactical Z-Score
            ((pl.col("close") - pl.col("sma_50")) / pl.col("std_50")).alias("z_score"),
            # Strategic Distance to 200d SMA %
            (((pl.col("close") / pl.col("sma_200")) - 1) * 100).alias("dist_200_pct"),
            # Annualized Volatility %
            (
                (pl.col("daily_return").rolling_std(window_size=200).over("ticker"))
                * (TRADING_DAYS_PER_YEAR**0.5)
                * 100
            ).alias("vola_annual_pct"),
        )
        # 3. Limit Calculator Logic (Future Lows)
        .with_columns(
            pl.col("low")
            .rolling_min(window_size=window_days)
            .shift(-window_days)
            .over("ticker")
            .alias("future_min_low")
        )
        # 4. Max Discount Calculation
        .with_columns(
            ((pl.col("close") - pl.col("future_min_low")) / pl.col("close") * 100).alias(
                "max_possible_discount_pct"
            )
        )
    )
    return selected_price_data


def calculate_ticker_status(
    df_data: pl.DataFrame,
    selected_tickers: list[str],
) -> tuple[pl.DataFrame, dict[str, tuple[float | None, float | None]]]:
    date_3y_ago = df_data.select(pl.col("date").max()).item() - relativedelta(years=3)

    # Ensure data is sorted by ticker and date
    df_window = df_data.filter(
        pl.col("ticker").is_in(selected_tickers) & (pl.col("date") >= date_3y_ago)
    ).sort(["ticker", "date"])

    df_result = df_window.group_by("ticker").agg(
        [
            # Current Values
            pl.col("z_score").last().alias("z_score"),
            pl.col("dist_200_pct").last().alias("trend_dist"),
            pl.col("vola_annual_pct").last().alias("vola_annual_pct"),
            pl.col("close").last().alias("price"),
            pl.col("currency").last().alias("currency"),
            # Historical Context
            pl.col("dist_200_pct").quantile(0.10).alias("p10_dist"),
            pl.col("dist_200_pct").quantile(0.90).alias("p90_dist"),
            pl.count().alias("data_points"),
            # Percentile Rank Calculation
            (
                (pl.col("dist_200_pct") < pl.col("dist_200_pct").last())
                .mean()
                .alias("valuation_rank")
            ),
        ]
    )
    df_final = df_result.with_columns(
        pl.when(pl.col("data_points") >= 100)
        .then(pl.col("valuation_rank"))
        .otherwise(None)
        .alias("valuation_rank"),
        pl.format("{} {}", pl.col("price"), pl.col("currency")).alias("price"),
    )
    corridor_rows = (
        df_final.filter(pl.col("data_points") > 100)
        .select("ticker", "p10_dist", "p90_dist")
        .to_dicts()
    )

    ticker_corridors = {row["ticker"]: (row["p10_dist"], row["p90_dist"]) for row in corridor_rows}

    # Fill missing tickers with (None, None) to match original contract
    for t in selected_tickers:
        if t not in ticker_corridors:
            ticker_corridors[t] = (None, None)

    return df_final, ticker_corridors


def format_limit(price: float, pct: float, currency_sym: str) -> str:
    limit_val = price * (1 - pct / 100)
    return f"{limit_val:.2f} {currency_sym} (-{pct:.1f}%)"


def calculate_limit_recommendation_data(
    selected_price_data: pl.DataFrame,
    df_status: pl.DataFrame,
    selected_tickers: list[str],
    show_in_eur: bool,
) -> pl.DataFrame:
    limit_data = []

    df_latest = (
        selected_price_data.sort(["ticker", "date"])
        .group_by("ticker")
        .last()
        .join(df_status.select(["ticker", "valuation_rank"]), on="ticker", how="left")
    )
    for ticker in selected_tickers:
        df_t = selected_price_data.filter(pl.col("ticker") == ticker).drop_nulls(
            subset=["max_possible_discount_pct"]
        )

        if df_t.height < 50:
            continue

        # Quantiles
        pct_safe = df_t.select(pl.col("max_possible_discount_pct").quantile(0.10)).item()
        pct_balanced = df_t.select(pl.col("max_possible_discount_pct").quantile(0.50)).item()
        pct_aggressive = df_t.select(pl.col("max_possible_discount_pct").quantile(0.75)).item()

        # Current Prices
        curr_row = df_latest.filter(pl.col("ticker") == ticker)
        if show_in_eur:
            base_price = curr_row["close_EUR"].item()
            sma_50 = curr_row["sma_50_EUR"].item()
            sma_200 = curr_row["sma_200_EUR"].item()
            currency_sym = "EUR"
        else:
            base_price = curr_row["close"].item()
            sma_50 = curr_row["sma_50"].item()
            sma_200 = curr_row["sma_200"].item()
            currency_sym = curr_row["currency"].item()

        curr_rank = curr_row["valuation_rank"].item()  # für Coloring
        curr_zscore = curr_row["z_score"].item()

        limit_data.append(
            {
                "ticker": ticker,
                "current": f"{base_price:.2f} {currency_sym}",
                "valuation_rank": curr_rank,
                "sma_200": f"{sma_200:.2f} {currency_sym}",
                "z_score": curr_zscore,
                "sma_50": f"{sma_50:.2f} {currency_sym}",
                "safe": format_limit(base_price, pct_safe, currency_sym),
                "balanced": format_limit(base_price, pct_balanced, currency_sym),
                "aggressive": format_limit(base_price, pct_aggressive, currency_sym),
            }
        )
    df_limits = pl.DataFrame(limit_data)

    return df_limits
