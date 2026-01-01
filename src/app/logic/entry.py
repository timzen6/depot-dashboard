import polars as pl
from dateutil.relativedelta import relativedelta


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
                * (252**0.5)
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
    ticker_corridors = {}
    status_data = []
    date_3y_ago = df_data.select(pl.col("date").max()).item() - relativedelta(years=3)

    # Ensure data is sorted by ticker and date
    df_data = df_data.sort(["ticker", "date"])

    for ticker in selected_tickers:
        curr_row = df_data.filter(pl.col("ticker") == ticker).tail(1)
        if curr_row.is_empty():
            continue

        curr_z = curr_row["z_score"].item()
        curr_dist = curr_row["dist_200_pct"].item()
        curr_vola = curr_row["vola_annual_pct"].item()
        curr_price = curr_row["close"].item()
        curr_sym = curr_row["currency"].item()

        # --- PERCENTILE & CORRIDOR CALCULATION ---
        # We look at the last 3 years (or all available data)
        history_df = df_data.filter(
            (pl.col("ticker") == ticker)
            & (pl.col("date") >= date_3y_ago)
            & (pl.col("dist_200_pct").is_not_null())
        )

        if history_df.height > 100:
            # Array of all historical distances
            hist_dists = history_df["dist_200_pct"]

            # 1. Percentile Rank (Current Status)
            percentile = (hist_dists < curr_dist).mean()

            # 2. Corridor Levels (for chart visualization)
            p10_dist = hist_dists.quantile(0.10)  # 10% Quantile (Cheap)
            # add maybe
            p90_dist = hist_dists.quantile(0.90)  # 90% Quantile (Expensive)
            ticker_corridors[ticker] = (p10_dist, p90_dist)

        else:
            # Fallback for new IPOs or too little data
            percentile = None
            ticker_corridors[ticker] = (None, None)

        status_data.append(
            {
                "ticker": ticker,
                "price": f"{curr_price:.2f} {curr_sym}",
                "z_score": curr_z,
                "trend_dist": curr_dist if curr_dist else None,
                "valuation_rank": percentile,
                "vola_annual_pct": curr_vola,
            }
        )

    df_status = pl.DataFrame(status_data)
    return df_status, ticker_corridors


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
            currency_sym = "EUR"
        else:
            base_price = curr_row["close"].item()
            currency_sym = curr_row["currency"].item()

        curr_rank = curr_row["valuation_rank"].item()  # für Coloring
        curr_zscore = curr_row["z_score"].item()

        limit_data.append(
            {
                "ticker": ticker,
                "valuation_rank": curr_rank,
                "z_score": curr_zscore,
                "current": f"{base_price:.2f} {currency_sym}",
                "safe": format_limit(base_price, pct_safe, currency_sym),
                "balanced": format_limit(base_price, pct_balanced, currency_sym),
                "aggressive": format_limit(base_price, pct_aggressive, currency_sym),
            }
        )
    df_limits = pl.DataFrame(limit_data)

    return df_limits
