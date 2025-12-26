from pathlib import Path
from typing import Any

import polars as pl
import yaml
from loguru import logger

from src.core.domain_models import AllocationItem, ETFComposition, ETFHolding, Sector
from src.core.normalization import sector_normalization


class ETFLoader:
    """
    Loads ETF compositions from a directory structure of YAML files.
    Example: config/etfs/world/msci_world.yaml
    """

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self._cache: dict[str, ETFComposition] = {}
        self._loaded = False

    def load(self) -> None:
        """Recursively loads all .yaml/.yml files in config_dir."""
        if self._loaded:
            return

        if not self.config_dir.exists():
            logger.warning(f"ETF Config directory not found at {self.config_dir}")
            return

        # Finde alle .yaml und .yml files rekursiv
        files = list(self.config_dir.rglob("*.yaml")) + list(self.config_dir.rglob("*.yml"))

        count = 0
        for file_path in files:
            try:
                with open(file_path, encoding="utf-8") as f:
                    raw_data = yaml.safe_load(f) or {}

                # File kann mehrere Ticker enthalten oder nur einen
                for ticker, data in raw_data.items():
                    self._parse_and_cache(ticker, data)
                    count += 1

            except Exception as e:
                logger.error(f"Error loading ETF file {file_path}: {e}")

        self._loaded = True
        logger.info(f"Loaded {count} ETF compositions from {len(files)} files in {self.config_dir}")

    def _parse_and_cache(self, ticker: str, data: dict[str, Any]) -> None:
        """Helper to parse raw dict into Pydantic model."""
        try:
            is_percent = data.get("weight_format") == "percent"
            divisor = 100.0 if is_percent else 1.0
            sectors = []
            for k, v in data.get("sectors", {}).items():
                norm_sector = sector_normalization(k)
                if norm_sector is None:
                    norm_sector = Sector.OTHER
                sectors.append(AllocationItem(category=norm_sector, weight=v / divisor))
            countries = [
                AllocationItem(category=k, weight=v / divisor)
                for k, v in data.get("countries", {}).items()
            ]
            holdings = []
            for h in data.get("top_holdings", []):
                weight = h.get("weight", 0.0) / divisor
                holding = ETFHolding(
                    ticker=h.get("ticker", ""),
                    name=h.get("name", ""),
                    weight=weight,
                )
                holdings.append(holding)

            comp = ETFComposition(
                ticker=ticker,
                name=data.get("name", ticker),
                ter=data.get("ter", 0.0),
                strategy=data.get("strategy"),
                sector_weights=sectors,
                country_weights=countries,
                top_holdings=holdings,
            )
            self._cache[ticker] = comp
        except Exception as e:
            logger.error(f"Validation error for {ticker}: {e}")

    def get(self, ticker: str) -> ETFComposition | None:
        if not self._loaded:
            self.load()
        return self._cache.get(ticker)

    # --- Bulk Data Access for Analytics ---

    def get_all_sectors(self) -> pl.DataFrame:
        """Returns a consolidated DataFrame of ALL ETF sectors."""
        if not self._loaded:
            self.load()
        dfs = [comp.sectors_df for comp in self._cache.values()]
        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs)

    def get_all_countries(self) -> pl.DataFrame:
        """Returns a consolidated DataFrame of ALL ETF countries."""
        if not self._loaded:
            self.load()
        dfs = [comp.countries_df for comp in self._cache.values()]
        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs)

    def get_all_top_holdings(self) -> pl.DataFrame:
        """Returns a consolidated DataFrame of ALL ETF top holdings."""
        if not self._loaded:
            self.load()
        dfs = [comp.top_holdings_df for comp in self._cache.values()]
        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs)
