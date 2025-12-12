"""Application configuration using Pydantic V2."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings and configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = Field(default="quality-core", description="Application name")
    app_version: str = Field(default="0.1.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    # Project paths
    project_root: Path = Field(default_factory=lambda: Path(__file__).parent.parent.parent)
    data_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent.parent / "data")

    # Data directories
    @property
    def raw_data_dir(self) -> Path:
        """Raw data directory."""
        return self.data_dir / "raw"

    @property
    def staging_data_dir(self) -> Path:
        """Staging data directory."""
        return self.data_dir / "staging"

    @property
    def production_data_dir(self) -> Path:
        """Production data directory."""
        return self.data_dir / "production"


# Singleton instance
settings = Settings()
