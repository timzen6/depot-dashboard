from typing import Any

from pydantic import BaseModel, Field, model_validator


class StrategyFactors(BaseModel):
    """Holds factor definitions for strategy engine."""

    tech: float = Field(default=0, ge=0, description="Innovation, Growth, R&D")
    stab: float = Field(default=0, ge=0, description="Stability, Low Vol, Recurring Revenue")
    real: float = Field(default=0, ge=0, description="Real Assets, Industry, Cyclical")
    price: float = Field(default=0, ge=0, description="Pricing Power, Brand, Moat")

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        """Ensure all factors are present, defaulting to 0."""
        if isinstance(data, dict):
            return data
        return data

    @model_validator(mode="after")
    def normalize_self(self) -> "StrategyFactors":
        total = self.tech + self.stab + self.real + self.price
        if total == 0:
            return self
        # in-place normalization
        self.tech /= total
        self.stab /= total
        self.real /= total
        self.price /= total
        return self

    def to_dict(self) -> dict[str, float]:
        """Convert factors to dictionary."""
        return {k: v for k, v in self.model_dump().items() if v > 0.001}

    def __add__(self, other: "StrategyFactors") -> "StrategyFactors":
        return StrategyFactors(
            tech=self.tech + other.tech,
            stab=self.stab + other.stab,
            real=self.real + other.real,
            price=self.price + other.price,
        )
