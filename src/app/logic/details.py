from dataclasses import dataclass

import polars as pl

from src.core.stock_data import StockData


@dataclass
class StockDetailContext:
    ticker: str
    name: str

    current_price: float | None
    daily_change_pct: float | None
    quality_score: float | None

    data: StockData


class StockDetailsLogic:
    def __init__(self, df_prices: pl.DataFrame, df_fundamentals: pl.DataFrame):
        self.df_prices = df_prices
        self.df_fundamentals = df_fundamentals

    def get_context(self, ticker: str) -> StockDetailContext:
        stock = StockData(
            ticker=ticker,
            prices=self.df_prices,
            fundamentals=self.df_fundamentals,
        )
        current_price = stock.latest_price
        last_two_days = stock.prices.tail(2)
        change_pct = None
        if last_two_days.height == 2:
            prev_close = last_two_days.get_column("close").item(0)
            curr_close = last_two_days.get_column("close").item(1)
            if prev_close > 0 and curr_close > 0:
                change_pct = ((curr_close - prev_close) / prev_close) * 100

        # Placeholder for quality score calculation
        quality_score = None

        return StockDetailContext(
            ticker=ticker,
            name=ticker,  # TODO: Replace with actual name lookup
            current_price=current_price,
            daily_change_pct=change_pct,
            quality_score=quality_score,
            data=stock,
        )
