"""Portfolio valuation engine for historical performance tracking."""

from datetime import date

import polars as pl
from loguru import logger

from src.analysis.fx import FXEngine
from src.config.models import Portfolio, PortfolioType


class PortfolioEngine:
    """Calculates historical portfolio values based on strategy type."""

    def calculate_portfolio_history(
        self,
        portfolio: Portfolio,
        df_prices: pl.DataFrame,
        fx_engine: FXEngine | None = None,
    ) -> pl.DataFrame:
        """Calculate daily portfolio values using specified strategy.

        Args:
            portfolio: Portfolio configuration with positions and strategy
            df_prices: Price data with columns [date, ticker, close, currency]

        Returns:
            DataFrame with columns [date, ticker, position_value, currency]
            For aggregated strategies (weighted), also includes total_value column

        Strategy implementations:
        - ABSOLUTE: position_value = shares * close
        - WEIGHTED: Simulate buy-and-hold from start_date with initial capital allocation
        - WATCHLIST: Return raw price data for tracking
        """
        logger.info(
            f"Calculating history for portfolio '{portfolio.name}' ({portfolio.type.value})"
            f" Tickers: {portfolio.tickers}"
        )

        # Filter to portfolio tickers
        df_portfolio = df_prices.filter(pl.col("ticker").is_in(portfolio.tickers))
        ticker_to_group = {pos.ticker: getattr(pos, "group", None) for pos in portfolio.positions}

        if df_portfolio.is_empty():
            logger.warning(f"No price data found for portfolio '{portfolio.name}'")
            return pl.DataFrame()

        # Apply start_date filter if provided
        if portfolio.start_date:
            start_date = date.fromisoformat(portfolio.start_date)
            df_portfolio = df_portfolio.filter(pl.col("date") >= start_date)
            logger.debug(f"Filtered to dates >= {start_date}")

        # Route to strategy-specific calculation
        if portfolio.type == PortfolioType.ABSOLUTE:
            history = self._calculate_absolute(portfolio, df_portfolio)
        elif portfolio.type == PortfolioType.WEIGHTED:
            history = self._calculate_weighted(portfolio, df_portfolio, fx_engine)
        else:  # WATCHLIST
            history = self._calculate_watchlist(df_portfolio)

        return history.with_columns(
            pl.col("ticker")
            .map_elements(lambda t: ticker_to_group.get(t, None), return_dtype=pl.Utf8)
            .fill_null("N/A")
            .alias("group")
        )

    def _calculate_absolute(self, portfolio: Portfolio, df_prices: pl.DataFrame) -> pl.DataFrame:
        """Calculate absolute portfolio: fixed share counts.

        position_value = shares * close
        """
        logger.debug(f"Calculating absolute strategy with {len(portfolio.positions)} positions")

        # Create mapping of ticker -> shares
        shares_map = {pos.ticker: pos.shares for pos in portfolio.positions}

        # Add shares column via mapping
        result = df_prices.with_columns(
            pl.col("ticker")
            .map_elements(lambda t: shares_map.get(t, 0.0), return_dtype=pl.Float64)
            .alias("shares")
        ).with_columns(
            (pl.col("shares") * pl.col("close")).alias("position_value"),
            (pl.col("shares") * pl.col("rolling_dividend_sum")).alias("position_dividend_yoy"),
        )

        logger.success(f"Calculated absolute portfolio: {result.height} records")
        return result.select(
            [
                "date",
                "ticker",
                "position_value",
                "position_dividend_yoy",
                "currency",
                "shares",
            ]
        )

    def _calculate_weighted(
        self,
        portfolio: Portfolio,
        df_prices: pl.DataFrame,
        fx_engine: FXEngine | None = None,
    ) -> pl.DataFrame:
        """Calculate weighted portfolio: buy-and-hold simulation.

        Steps:
        1. Get prices at start_date for each ticker
        2. Calculate implied shares: (initial_capital * weight) / start_price
        3. Project forward: position_value = implied_shares * daily_close
        """
        logger.debug(f"Calculating weighted strategy (capital: {portfolio.initial_capital})")

        if portfolio.start_date is None:
            raise ValueError("Portfolio start_date is required for weighted strategy")
        start_date = date.fromisoformat(portfolio.start_date)

        # Get start prices for each ticker
        df_start = (
            df_prices.filter(pl.col("date") >= start_date)
            # get first available price on or after start_date
            # in case of missing data or start_date on non-trading day
            .sort("date")
            .group_by("ticker")
            # take first record per ticker (all data)
            .agg(pl.all().first())
            .select(["ticker", "close", "currency", "date"])
            .rename({"close": "start_price"})
        )

        if df_start.is_empty():
            logger.warning(f"No price data found for start_date {start_date}")
            return pl.DataFrame()

        # Convert start prices to target currency if FX engine provided
        if fx_engine is not None:
            df_start = fx_engine.convert_to_target(
                df_start, amount_col="start_price", source_currency_col="currency"
            ).rename({f"start_price_{fx_engine.target_currency}": "start_price_adjusted"})
        else:
            df_start = df_start.with_columns(pl.col("start_price").alias("start_price_adjusted"))

        # Create positions DataFrame with weights
        capital = portfolio.initial_capital or 0.0
        positions_data = [
            {
                "ticker": pos.ticker,
                "weight": pos.weight or 0.0,
            }
            for pos in portfolio.positions
        ]
        df_positions = (
            pl.DataFrame(positions_data)
            .with_columns(
                # here we normalize weights to sum to 1.0, so that we can use
                # ratios when defining allocations, thats much easier to read
                (pl.col("weight") / pl.col("weight").sum()).alias("weight"),
            )
            .with_columns(
                (pl.col("weight") * capital).alias("allocation"),
            )
        )

        # Calculate implied shares at start
        df_shares = (
            df_positions.join(df_start, on="ticker", how="left")
            .with_columns(
                (pl.col("allocation") / pl.col("start_price_adjusted")).alias("implied_shares")
            )
            .select(["ticker", "implied_shares", "weight"])
        )

        # Join with full price history and calculate position values
        result = (
            df_prices.join(df_shares, on="ticker", how="left")
            .with_columns(
                (pl.col("implied_shares") * pl.col("close")).alias("position_value"),
                (pl.col("implied_shares") * pl.col("rolling_dividend_sum")).alias(
                    "position_dividend_yoy"
                ),
            )
            .select(
                [
                    "date",
                    "ticker",
                    "position_value",
                    "currency",
                    "implied_shares",
                    "weight",
                    "position_dividend_yoy",
                ]
            )
        )

        logger.success(f"Calculated weighted portfolio: {result.height} records")
        return result

    def _calculate_watchlist(self, df_prices: pl.DataFrame) -> pl.DataFrame:
        """Watchlist: just return raw price data for tracking."""
        logger.debug(f"Watchlist mode: returning {df_prices.height} price records")

        return df_prices.select(
            ["date", "ticker", "close", "currency", "rolling_dividend_sum"]
        ).rename({"close": "position_value", "rolling_dividend_sum": "position_dividend_yoy"})

    def aggregate_total_value(self, df_portfolio: pl.DataFrame) -> pl.DataFrame:
        """Aggregate position values to daily total portfolio value.

        Groups by date and sums position_value across all tickers.
        Useful for weighted and absolute portfolios.

        Args:
            df_portfolio: Output from calculate_portfolio_history()

        Returns:
            DataFrame with columns [date, total_value]
        """
        if "position_value" not in df_portfolio.columns:
            logger.warning("Cannot aggregate: position_value column missing")
            return pl.DataFrame()

        return (
            df_portfolio.group_by("date")
            .agg(pl.col("position_value").sum().alias("total_value"))
            .sort("date")
        )
