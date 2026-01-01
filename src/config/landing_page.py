### src/config/landing_page.py
from pathlib import Path
from typing import Literal

import yaml
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field


# --- Factor Models ---
class FactorDefinition(BaseModel):
    """Defines the philosophy behind a strategy factor."""

    model_config = ConfigDict(frozen=True)

    title: str = Field(..., description="Display title (e.g. 'Stabilität & Wiederkehr')")
    icon: str = Field(default="ℹ️")
    description: str
    test_question: str = Field(..., description="The litmus test question")
    indicators: str = Field(..., description="Key metrics to check")
    examples: str = Field(..., description="Example stocks that fit the factor")


# --- Strategy Models ---
class StrategyComponent(BaseModel):
    """A sub-component of a strategy (e.g., 'Global Base ETF')."""

    model_config = ConfigDict(frozen=True)
    name: str
    detail: str


class ETFStrategy(BaseModel):
    """The ETF / Foundation strategy definition."""

    model_config = ConfigDict(frozen=True)
    name: str
    allocation_target: str
    description: str
    components: list[StrategyComponent] = Field(default_factory=list)


class StockStrategy(BaseModel):
    """The Stock / Quality Core strategy definition."""

    model_config = ConfigDict(frozen=True)
    name: str
    allocation_target: str
    description: str
    pillars: list[StrategyComponent] = Field(default_factory=list)


class StrategyConfig(BaseModel):
    """Wrapper for the two main strategy arms."""

    model_config = ConfigDict(frozen=True)
    foundation: ETFStrategy
    quality_core: StockStrategy


# --- Execution Models ---
class ExecutionRule(BaseModel):
    """A general rule for buying/selling."""

    model_config = ConfigDict(frozen=True)
    title: str
    text: str


# --- Data Models (Existing) ---
class AlertDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str
    action: Literal["buy", "sell", "hold"]
    metric: str
    fair_threshold: float | None = None
    good_threshold: float
    comment: str | None = None


class PriceAlarmDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)
    ticker: str
    level_1: float
    # Optionally set two levels
    level_2: float | None = None
    direction: Literal["above", "below"]
    sentiment: Literal["negative", "neutral", "positive"] = "neutral"
    price_type: Literal["close", "low", "high"] = "close"


# --- ROOT CONFIG ---
class LandingPageConfig(BaseModel):
    """
    Root configuration model for the Dashboard Start Page.
    """

    model_config = ConfigDict(frozen=True)

    factors: dict[str, FactorDefinition] = Field(default_factory=dict)
    strategy: StrategyConfig | None = None  # Optional to avoid crash if YAML part missing
    execution: list[ExecutionRule] = Field(default_factory=list)

    watchlist_tickers: list[str] = Field(default_factory=list)
    alerts: list[AlertDefinition] = Field(default_factory=list)
    price_alarms: list[PriceAlarmDefinition] = Field(default_factory=list)


def load_landing_page_config(
    config_path: Path = Path("config/landing_page.yaml"),
) -> LandingPageConfig:
    if not config_path.exists():
        logger.warning(f"Landing page config not found at {config_path}. Using defaults.")
        return LandingPageConfig()

    try:
        with open(config_path, encoding="utf-8") as f:
            raw_data = yaml.safe_load(f) or {}
        return LandingPageConfig(**raw_data)

    except Exception as e:
        logger.error(f"Failed to load landing page config: {e}")
        return LandingPageConfig()
