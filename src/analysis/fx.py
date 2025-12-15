"""Foreign exchange conversion engine.

Handles multi-currency portfolio conversions using historical FX rates.
"""

import polars as pl
from loguru import logger


class FXEngine:
    """Convert asset values between currencies using historical rates."""

    SUPPORTED_CURRENCY_TICKER = {
        "USD": "EURUSD=X",
        "CHF": "EURCHF=X",
        "GBP": "EURGBP=X",
        "JPY": "EURJPY=X",
        "DKK": "EURDKK=X",
        "SEK": "EURSEK=X",
    }

    def __init__(self, df_prices: pl.DataFrame, target_currency: str = "EUR"):
        """Initialize FX engine with available exchange rates.

        Args:
            df_prices: Price data including FX rate tickers (e.g., EURUSD=X)
            target_currency: Reporting currency for conversions
        """
        self.target_currency = target_currency
        self.fx_rates = self._extract_rates(df_prices)

    def _extract_rates(self, df_prices: pl.DataFrame) -> dict[str, pl.DataFrame]:
        """Extract FX rate time series from price data.

        Returns:
            Dict mapping currency codes to rate DataFrames [date, rate]
        """
        if self.target_currency != "EUR":
            raise NotImplementedError("Only EUR target currency is implemented.")

        rates = {}

        for currency, ticker in self.SUPPORTED_CURRENCY_TICKER.items():
            df_rate = (
                df_prices.filter(pl.col("ticker") == ticker)
                .select(["date", "close"])
                .rename({"close": "rate"})
                .sort("date")
            )
            if not df_rate.is_empty():
                rates[currency] = df_rate

        return rates

    def convert_to_target(
        self, df: pl.DataFrame, amount_col: str, source_currency_col: str
    ) -> pl.DataFrame:
        """Convert amount column to target currency using historical rates.

        Adds new column: {amount_col}_{target_currency}

        Args:
            df: DataFrame with date, amount, and currency columns
            amount_col: Column name containing values to convert
            source_currency_col: Column name containing currency codes

        Returns:
            DataFrame with converted amount column added
        """
        target_col = f"{amount_col}_{self.target_currency}"

        # Split into home currency (no conversion) and foreign
        df_home = df.filter(pl.col(source_currency_col) == self.target_currency).with_columns(
            pl.col(amount_col).alias(target_col)
        )

        df_foreign = df.filter(pl.col(source_currency_col) != self.target_currency)

        # Log currencies needing conversion
        foreign_currencies = (
            df_foreign.select(pl.col(source_currency_col).unique()).to_series().to_list()
        )
        if foreign_currencies:
            logger.info(f"Converting currencies: {foreign_currencies}")

        # Warn about unsupported currencies
        for currency in foreign_currencies:
            if currency not in self.fx_rates:
                logger.warning(
                    f"No FX rate available for {currency} → {self.target_currency}, "
                    "values will be kept in original currency"
                )

        # Convert supported currencies
        converted_chunks = []
        for currency, df_rate in self.fx_rates.items():
            df_currency = df_foreign.filter(pl.col(source_currency_col) == currency)

            if not df_currency.is_empty():
                # Join with FX rates using asof strategy to get last available rate
                df_converted = (
                    df_currency.sort("date")
                    .join_asof(df_rate, on="date", strategy="backward")
                    .with_columns(
                        # Division: 100 USD / 1.10 EUR/USD = 90.91 EUR
                        (pl.col(amount_col) / pl.col("rate")).alias(target_col)
                    )
                    .drop("rate")
                )
                converted_chunks.append(df_converted)

        # Handle unsupported currencies: keep original values
        unsupported_currencies = [c for c in foreign_currencies if c not in self.fx_rates]
        if unsupported_currencies:
            df_unsupported = df_foreign.filter(
                pl.col(source_currency_col).is_in(unsupported_currencies)
            ).with_columns(pl.col(amount_col).alias(target_col))
            converted_chunks.append(df_unsupported)

        # Combine all chunks
        return pl.concat([df_home] + converted_chunks, how="vertical_relaxed").sort("date")

    def convert_amount(self, amount: float, date: pl.Date, source_currency: str) -> float:
        """Convert a single amount on a specific date to the target currency.

        Args:
            amount: Amount in source currency
            date: Date for FX rate lookup
            source_currency: Currency code of the amount

        Returns:
            Converted amount in target currency
        """
        if source_currency == self.target_currency:
            return amount

        if source_currency not in self.fx_rates:
            logger.warning(
                f"No FX rate available for {source_currency} → {self.target_currency}, "
                "returning original amount"
            )
            return amount

        df_rate = self.fx_rates[source_currency]
        rate_row = df_rate.filter(pl.col("date") <= date).sort("date", descending=True).head(1)

        if rate_row.is_empty():
            logger.warning(
                f"No FX rate found for {source_currency} on or before {date}, "
                "returning original amount"
            )
            return amount

        rate = rate_row.select(pl.col("rate")).item()
        converted_amount = amount / rate
        return float(converted_amount)
