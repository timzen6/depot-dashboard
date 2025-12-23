from pathlib import Path

import polars as pl
import yaml
from loguru import logger

from src.core.strategy_models import StrategyFactors


class StrategyEngine:
    def __init__(self, config_path: Path = Path("config/factors.yaml")) -> None:
        self.config_path = config_path
        self.defaults: dict[str, StrategyFactors] = {}
        self.overrides: dict[str, StrategyFactors] = {}
        self._load_config()

    def _load_config(self) -> None:
        if not self.config_path.exists():
            logger.warning(f"Strategy factors config not found at {self.config_path}")
            return

        with open(self.config_path) as f:
            config = yaml.safe_load(f)

        raw_defaults = config.get("defaults", {})
        for sector, factors in raw_defaults.items():
            self.defaults[sector] = StrategyFactors(**factors)
        raw_overrides = config.get("overrides", {})
        for ticker, factors in raw_overrides.items():
            self.overrides[ticker] = StrategyFactors(**factors)

    def get_factor_profile(self, ticker: str, sector: str) -> StrategyFactors:
        if ticker in self.overrides:
            return self.overrides[ticker]
        if sector in self.defaults:
            return self.defaults[sector]
        return StrategyFactors()

    def join_factor_profiles(
        self,
        df_positions: pl.DataFrame,
        sector_column: str = "sector",
    ) -> pl.DataFrame:
        relevant_profiles = []

        for row in df_positions.to_dicts():
            ticker = row["ticker"]
            sector = row.get(sector_column, "")
            profile = self.get_factor_profile(ticker, sector)
            profile_dict = profile.to_dict()
            profile_dict["ticker"] = ticker
            relevant_profiles.append(profile_dict)

        df_profiles = pl.DataFrame(relevant_profiles)
        df_result = df_positions.join(df_profiles, on="ticker", how="left")
        return df_result

    def calculate_portfolio_exposure(
        self,
        df_positions: pl.DataFrame,
        value_column: str = "market_value",
        sector_column: str = "sector",
    ) -> pl.DataFrame:
        df_joined = self.join_factor_profiles(df_positions, sector_column=sector_column)
        value_sums = (
            df_joined.with_columns(
                (pl.col("tech").fill_null(0) * pl.col(value_column)).alias("tech_value"),
                (pl.col("stab").fill_null(0) * pl.col(value_column)).alias("stab_value"),
                (pl.col("real").fill_null(0) * pl.col(value_column)).alias("real_value"),
                (pl.col("price").fill_null(0) * pl.col(value_column)).alias("price_value"),
            )
            .select([value_column, "tech_value", "stab_value", "real_value", "price_value"])
            .sum()
        )

        total_portfolio_value = value_sums.select(pl.col(value_column)).item()
        total_tech = value_sums.select(pl.col("tech_value")).item()
        total_stab = value_sums.select(pl.col("stab_value")).item()
        total_real = value_sums.select(pl.col("real_value")).item()
        total_price = value_sums.select(pl.col("price_value")).item()
        total_unclassified = total_portfolio_value - (
            total_tech + total_stab + total_real + total_price
        )

        results = [
            {"key": "tech", "factor": "Technology / Innovation", "value": total_tech},
            {"key": "stab", "factor": "Stability / Defensive", "value": total_stab},
            {"key": "real", "factor": "Real Assets / Industry", "value": total_real},
            {"key": "price", "factor": "Pricing Power / Brand", "value": total_price},
            {
                "key": "unclassified",
                "factor": "Unclassified",
                "value": total_unclassified,
            },
        ]
        df_result = (
            pl.DataFrame(results)
            .with_columns((pl.col("value") / total_portfolio_value).alias("proportion"))
            .sort("value", descending=True)
        )
        return df_result
