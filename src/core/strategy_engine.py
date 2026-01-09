from pathlib import Path

import polars as pl
import yaml
from loguru import logger

from src.core.strategy_models import StrategyFactors


def safe_column_expr(df: pl.DataFrame, col_name: str) -> pl.Expr:
    if col_name in df.columns:
        return pl.col(col_name)
    return pl.lit(0).alias(col_name)


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

    @property
    def factor_mapping(self) -> dict[str, str]:
        return {
            "tech": "Technology",
            "stab": "Stability",
            "real": "Real Assets",
            "price": "Pricing Power",
        }

    @property
    def factor_emoji_mapping(self) -> dict[str, str]:
        return {
            "tech": "🔬",
            "stab": "🛡️",
            "real": "⚙️",
            "price": "👜",
        }

    def get_sector_reference(self, sector: str) -> StrategyFactors:
        if sector in self.defaults:
            return self.defaults[sector]
        return StrategyFactors()

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
        include_zero: bool = False,
        include_sector_reference: bool = False,
    ) -> pl.DataFrame:
        relevant_profiles = []
        relevant_sector_references = []

        if df_positions.is_empty():
            return df_positions

        for row in df_positions.to_dicts():
            ticker = row["ticker"]
            sector = row.get(sector_column, "")
            profile = self.get_factor_profile(ticker, sector)
            profile_dict = profile.to_dict(include_zero=include_zero)
            profile_dict["ticker"] = ticker
            relevant_profiles.append(profile_dict)
        if include_sector_reference:
            for row in df_positions.to_dicts():
                sector = row.get(sector_column, "")
                sector_ref = self.get_sector_reference(sector)
                sector_ref_dict = sector_ref.to_dict(include_zero=include_zero)
                sector_ref_dict["ticker"] = row["ticker"]
                relevant_sector_references.append(sector_ref_dict)
            df_sector_refs = pl.DataFrame(relevant_sector_references)
            df_profiles = pl.DataFrame(relevant_profiles)
            df_result = df_positions.join(df_profiles, on="ticker", how="left").join(
                df_sector_refs, on="ticker", how="left", suffix="_ref"
            )
            return df_result

        df_profiles = pl.DataFrame(relevant_profiles)
        if df_profiles.is_empty():
            return df_positions
        df_result = df_positions.join(df_profiles, on="ticker", how="left")
        return df_result

    def calculate_portfolio_exposure(
        self,
        df_positions: pl.DataFrame,
        value_column: str = "market_value",
        sector_column: str = "sector",
    ) -> pl.DataFrame:
        df_joined = self.join_factor_profiles(df_positions, sector_column=sector_column)
        col_expr = [
            safe_column_expr(df_joined, fac).fill_null(0) * pl.col(value_column)
            for fac in self.factor_mapping.keys()
        ]

        value_sums = (
            df_joined.with_columns(col_expr)
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
