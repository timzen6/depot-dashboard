"""Portfolio valuation engine for historical performance tracking."""

from datetime import date
from typing import Any

import polars as pl
from loguru import logger

from src.config.models import Portfolio, PortfolioType
from src.core.domain_models import TransactionType


class PortfolioEngine:
    """Calculates historical portfolio values based on strategy type."""

    def calculate_portfolio_history(
        self,
        portfolio: Portfolio,
        df_prices: pl.DataFrame,
    ) -> pl.DataFrame:
        """Calculate daily portfolio values using specified strategy.

        Args:
            portfolio: Portfolio configuration with positions and strategy
            df_prices: Price data with columns [date, ticker, close, currency]

        Returns:
            DataFrame with columns [date, ticker, position_value, currency]

        Strategy implementations:
        - ABSOLUTE: position_value = shares * close
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

        present_tickers = set(df_portfolio["ticker"].unique().to_list())
        missing_tickers = set(portfolio.tickers) - present_tickers
        if missing_tickers:
            logger.warning(
                f"Portfolio '{portfolio.name}': no price data for tickers:"
                f" {sorted(missing_tickers)}"
            )

        # Apply start_date filter if provided
        if portfolio.start_date:
            start_date = date.fromisoformat(portfolio.start_date)
            df_portfolio = df_portfolio.filter(pl.col("date") >= start_date)

        # Route to strategy-specific calculation
        if portfolio.type == PortfolioType.ABSOLUTE:
            history = self._calculate_absolute(portfolio, df_portfolio)
        elif portfolio.type == PortfolioType.TRANSACTIONAL:
            history = self._calculate_transactional(portfolio, df_portfolio)
        else:  # WATCHLIST
            history = self._calculate_watchlist(df_portfolio)

        return history.with_columns(
            pl.col("ticker")
            .map_elements(lambda t: ticker_to_group.get(t, None), return_dtype=pl.Utf8)
            .fill_null("N/A")
            .alias("group")
        )

    def _apply_splits(
        self,
        transactions: list[dict[str, Any]],
        relevant_splits: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        # go through all transactions and apply splits:
        # if there is a split for a ticker on a date,
        # all transactions for that ticker on that date or later are adjusted by the split factor
        for i, transaction in enumerate(transactions):
            ticker_splits = relevant_splits.get(transaction["ticker"], [])
            applicable_splits = [s for s in ticker_splits if s["date"] > transaction["date"]]
            if not applicable_splits:
                continue
            factor = 1.0
            for s in applicable_splits:
                factor *= s["stock_splits"]
            transactions[i]["delta"] *= factor
            if transaction["price"] is not None:
                transactions[i]["price"] /= factor
        return transactions

    def _get_relevant_splits(
        self, df_prices: pl.DataFrame, portfolio: Portfolio, start_date: date
    ) -> dict[str, list[dict[str, Any]]]:
        splits_df = (
            df_prices.filter(
                (pl.col("date") >= start_date)
                & (pl.col("ticker").is_in(portfolio.tickers))
                & (pl.col("stock_splits").is_not_null())
                & (pl.col("stock_splits") > 0)
            )
            .select(["date", "ticker", "stock_splits"])
            .sort(["ticker", "date"])
        )
        relevant_splits: dict[str, list[dict[str, Any]]] = {
            ticker: group.select(["date", "stock_splits"]).to_dicts()
            for (ticker,), group in splits_df.group_by("ticker")
        }
        if relevant_splits:
            logger.info(f"Found the following splits for portfolio '{portfolio.name}':")
            logger.info(splits_df.to_pandas().to_markdown())
        return relevant_splits

    def _add_fractional_share_sales(
        self,
        transactions: list[dict[str, Any]],
        stock_splits: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        """Simulate fractional share sell-offs that brokers execute after a split."""
        fractional_sells = []

        for ticker, splits in stock_splits.items():
            ticker_txs = [tx for tx in transactions if tx["ticker"] == ticker]

            # Merge trades and splits into one timeline. Splits sort before trades on
            # the same day ("split" < "trade") which mirrors pre-market split execution.
            timeline = [
                {"date": tx["date"], "type": "trade", "val": tx["delta"]} for tx in ticker_txs
            ]
            timeline.extend(
                {"date": s["date"], "type": "split", "val": s["stock_splits"]} for s in splits
            )
            timeline.sort(key=lambda x: (x["date"], x["type"]))

            current_shares = 0.0
            for event in timeline:
                if event["type"] == "trade":
                    current_shares += event["val"]
                elif event["type"] == "split":
                    current_shares = round(current_shares * event["val"], 6)
                    fraction = round(current_shares % 1.0, 6)
                    if fraction > 0.0001:
                        fractional_sells.append(
                            dict(
                                date=event["date"],
                                ticker=ticker,
                                delta=-fraction,
                                price=None,
                            )
                        )
                        current_shares -= fraction

        return transactions + fractional_sells

    def _calculate_transactional(
        self,
        portfolio: Portfolio,
        df_prices: pl.DataFrame,
    ) -> pl.DataFrame:
        transactions = portfolio.transactions
        transaction_events = []
        # start date: either portfolio start_date or earliest transaction date, whichever is earlier
        start_date = None
        if portfolio.start_date:
            start_date = date.fromisoformat(portfolio.start_date)
        if transactions:
            earliest_tx_date = min(tx.date for tx in transactions)
            if start_date is None:
                start_date = earliest_tx_date
            else:
                start_date = min(start_date, earliest_tx_date)

        if start_date is None:
            raise ValueError(
                "Cannot calculate transactional portfolio: no start_date and no transactions found."
            )

        relevant_splits = self._get_relevant_splits(df_prices, portfolio, start_date)

        # initial portfolio
        for pos in portfolio.positions:
            transaction_events.append(
                dict(
                    date=start_date,
                    ticker=pos.ticker,
                    delta=pos.shares,
                    price=None,
                )
            )
        if transactions:
            for tx in transactions:
                transaction_events.append(
                    dict(
                        date=tx.date,
                        ticker=tx.ticker,
                        delta=(tx.shares if tx.type == TransactionType.BUY else -tx.shares),
                        price=tx.price,
                    )
                )
        transaction_events = self._add_fractional_share_sales(
            transaction_events,
            relevant_splits,
        )
        transaction_events = self._apply_splits(transaction_events, relevant_splits)

        trading_days = (
            df_prices.select(pl.col("date").alias("mapped_date")).unique().sort("mapped_date")
        )
        df_shares = (
            pl.DataFrame(
                transaction_events,
                schema={
                    "date": pl.Date,
                    "ticker": pl.Utf8,
                    "delta": pl.Float64,
                    "price": pl.Float64,
                },
            )
            .sort("date")
            .join_asof(
                trading_days,
                left_on="date",
                right_on="mapped_date",
                strategy="forward",
            )
            .with_columns(pl.col("mapped_date").alias("date"))
            .drop("mapped_date")
            .sort(["ticker", "date"])
            .join_asof(
                df_prices.select(["date", "ticker", "close_EUR"]).sort(["ticker", "date"]),
                on="date",
                by="ticker",
                strategy="backward",
            )
            .with_columns(pl.coalesce([pl.col("price"), pl.col("close_EUR")]).alias("price"))
            .with_columns((pl.col("price") * pl.col("delta")).alias("cashflow_EUR"))
            .group_by(["ticker", "date"])
            .agg(pl.col("delta").sum(), pl.col("cashflow_EUR").sum())
            .sort(["ticker", "date"])
            .with_columns(pl.col("delta").cum_sum().over("ticker").alias("shares"))
            .select(["date", "ticker", "shares", "delta", "cashflow_EUR"])
        )

        # sanity check: negative shares are not supported and will be clipped to 0
        if (df_shares.get_column("shares").lt(0)).any():
            logger.warning("Negative shares found in transactional strategy, clipping to 0")
            df_shares = df_shares.with_columns(pl.col("shares").clip(lower_bound=0))

        unique_tickers = df_shares["ticker"].unique().to_list()
        missing_tickers = set(unique_tickers) - set(df_prices["ticker"].unique().to_list())
        if missing_tickers:
            logger.warning(
                f"Transactional portfolio '{portfolio.name}': no price data for tickers:"
                f" {sorted(missing_tickers)}"
            )

        result = (
            df_prices.filter(
                (pl.col("date") >= start_date) & (pl.col("ticker").is_in(unique_tickers))
            )
            .sort(["date", "ticker"])
            .join_asof(
                df_shares.select(["date", "ticker", "shares"]).sort(["date", "ticker"]),
                on="date",
                by="ticker",
                strategy="backward",
            )
            .join(
                df_shares.select(["date", "ticker", "cashflow_EUR"]).sort(["date", "ticker"]),
                on=["date", "ticker"],
                how="left",
            )
            .with_columns(
                pl.col("shares").fill_null(0.0).alias("shares"),
                pl.col("cashflow_EUR").fill_null(0.0).alias("cashflow_EUR"),
            )
        )

        return result.with_columns(
            # Later we could calculate the actual dividends correctly
            (pl.col("shares") * pl.col("close")).alias("position_value"),
            (pl.col("shares") * pl.col("dividend")).alias("position_dividend"),
        ).select(
            [
                "date",
                "ticker",
                "position_value",
                "position_dividend",
                "currency",
                "shares",
                "cashflow_EUR",
            ]
        )

    def _calculate_absolute(self, portfolio: Portfolio, df_prices: pl.DataFrame) -> pl.DataFrame:
        """Calculate absolute portfolio: fixed share counts.

        position_value = shares * close
        """
        return self._calculate_transactional(portfolio, df_prices)

    def _calculate_watchlist(self, df_prices: pl.DataFrame) -> pl.DataFrame:
        """Watchlist: just return raw price data for tracking."""
        return (
            df_prices.select(["date", "ticker", "close", "currency", "rolling_dividend_sum"])
            .with_columns(
                pl.lit(1.0).alias("shares"),
            )
            .rename(
                {
                    "close": "position_value",
                    "rolling_dividend_sum": "position_dividend_yoy",
                }
            )
        )
