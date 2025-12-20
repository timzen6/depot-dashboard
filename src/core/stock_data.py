from dataclasses import dataclass, field
from datetime import date

import polars as pl

from src.app.logic.data_loader import DashboardData


@dataclass
class StockData:
    ticker: str
    prices: pl.DataFrame
    fundamentals: pl.DataFrame
    metadata: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dataset(
        cls,
        ticker: str,
        data: DashboardData,
    ) -> "StockData":
        """
        Factory method to create StockData for a specific ticker
        """
        ticker_metadata = data.metadata.filter(pl.col("ticker") == ticker)
        if ticker_metadata.is_empty():
            metadata_dict = {}
        else:
            metadata_dict = ticker_metadata.row(0, named=True)

        return cls(
            ticker=ticker,
            prices=data.prices.filter(pl.col("ticker") == ticker).sort("date"),
            fundamentals=data.fundamentals.filter(pl.col("ticker") == ticker).sort("date"),
            metadata=metadata_dict,
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
