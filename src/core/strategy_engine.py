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
        # Accumulators
        total_tech = 0.0
        total_stab = 0.0
        total_real = 0.0
        total_price = 0.0
        total_unclassified = 0.0

        total_portfolio_value = 0
        for row in df_positions.to_dicts():
            ticker = row.get("ticker", "")
            sector = row.get(sector_column, "Unclassified")
            value = row.get(value_column, 0)

            total_portfolio_value += value
            profile = self.get_factor_profile(ticker, sector)

            weight_sum = profile.tech + profile.stab + profile.real + profile.price
            if weight_sum == 0:
                total_unclassified += value
            else:
                total_tech += profile.tech * value
                total_stab += profile.stab * value
                total_real += profile.real * value
                total_price += profile.price * value
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
