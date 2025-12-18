from dataclasses import dataclass
from datetime import date

import polars as pl


@dataclass
class StockData:
    ticker: str
    prices: pl.DataFrame
    fundamentals: pl.DataFrame

    @classmethod
    def from_dataset(
        cls, ticker: str, df_all_prices: pl.DataFrame, df_all_fundamentals: pl.DataFrame
    ) -> "StockData":
        """
        Factory method to create StockData for a specific ticker
        """
        return cls(
            ticker=ticker,
            prices=df_all_prices.filter(pl.col("ticker") == ticker).sort("date"),
            fundamentals=df_all_fundamentals.filter(pl.col("ticker") == ticker).sort("date"),
        )

    def filter_date_range(self, start_date: date, end_date: date) -> "StockData":
        """
        Filter prices and fundamentals to a specific date range
        """
        filtered_prices = self.prices.filter(pl.col("date").is_between(start_date, end_date))
        filtered_fundamentals = self.fundamentals.filter(
            pl.col("date").is_between(start_date, end_date)
        )
        return StockData(
            ticker=self.ticker,
            prices=filtered_prices,
            fundamentals=filtered_fundamentals,
        )

    @property
    def is_empty(self) -> bool:
        """
        Check if there is no price data available
        """
        return self.prices.is_empty()

    @property
    def latest_price(self) -> float | None:
        """
        Get the latest closing price
        """
        if self.prices.is_empty():
            return None
        latest_record = self.prices.sort("date").select("close").row(-1)
        return float(latest_record[0])
