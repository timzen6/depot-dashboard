from pathlib import Path

import polars as pl
import yaml
from loguru import logger

from src.config.models import Portfolio, PortfoliosConfig, PortfolioType, Position
from src.config.settings import Config, load_config
from src.core.domain_models import AssetType
from src.core.file_manager import ParquetStorage
from src.etl.extract import DataExtractor
from src.etl.pipeline import ETLPipeline
from src.etl.snapshot import make_snapshot


class UserPortfolioManager:
    def __init__(self, portfolio_dir: Path):
        self.file_path = portfolio_dir / "user_portfolios.yaml"
        portfolio_dir.mkdir(parents=True, exist_ok=True)

    def get_system_portfolios(self) -> dict[str, Portfolio]:
        config = load_config()
        # config.portfolios can be None, handle gracefully
        if config.portfolios is None or config.portfolios.portfolios is None:
            return {}
        return {k: v.model_copy(deep=True) for k, v in config.portfolios.portfolios.items()}

    def get_all_portfolios(self) -> dict[str, Portfolio]:
        final_portfolios = self.get_system_portfolios()

        logger.info(f"Loading user portfolios from {self.file_path}")
        if not self.file_path.exists():
            logger.warning(f"No user portfolios found at {self.file_path}")
            return final_portfolios
        with self.file_path.open("r") as f:
            raw_data = yaml.safe_load(f) or {"portfolios": {}}

        for name, pdata in raw_data.get("portfolios", {}).items():
            if name in final_portfolios:
                logger.warning(
                    f"User portfolio '{name}'. Name conflicts with system portfolio. " "Skipping."
                )
                continue
            pdata["is_editable"] = True
            try:
                final_portfolios[name] = Portfolio(**pdata)
            except Exception as e:
                logger.error(f"Error loading user portfolio '{name}': {e}. Skipping.")

        portfolio_config = PortfoliosConfig(portfolios=final_portfolios)
        return portfolio_config.portfolios

    def _dump_portfolios(self, portfolios: dict[str, Portfolio]) -> None:
        system_portfolios = self.get_system_portfolios()
        user_portfolios = {
            name: pmodel for name, pmodel in portfolios.items() if name not in system_portfolios
        }

        config = PortfoliosConfig(portfolios=user_portfolios)
        with self.file_path.open("w") as f:
            model_dump = config.model_dump(mode="json")
            yaml.safe_dump(model_dump, f)

    def create_portfolio(self, name: str, display_name: str | None = None) -> None:
        """Create a new user portfolio with the given name."""
        all_portfolios = self.get_all_portfolios()
        if name in all_portfolios:
            logger.error(f"Portfolio with name '{name}' already exists.")
            return
        new_portfolio = Portfolio(
            name=name,
            display_name=display_name,
            positions=[],
            type=PortfolioType.ABSOLUTE,
            is_editable=True,
        )
        all_portfolios[name] = new_portfolio
        self._dump_portfolios(all_portfolios)

    def delete_portfolio(self, portfolio_name: str) -> str:
        all_portfolios = self.get_all_portfolios()
        if portfolio_name not in all_portfolios:
            logger.error(f"Portfolio '{portfolio_name}' does not exist.")
            return "Error: Portfolio does not exist."
        portfolio = all_portfolios.get(portfolio_name)
        if portfolio is None:
            logger.error(f"Portfolio '{portfolio_name}' does not exist.")
            return "Error: Portfolio does not exist."
        if not getattr(portfolio, "is_editable", False):
            logger.error(f"Portfolio '{portfolio_name}' is not editable and cannot be deleted.")
            return "Error: Portfolio is not editable and cannot be deleted."
        del all_portfolios[portfolio_name]
        self._dump_portfolios(all_portfolios)
        return "Success: Portfolio deleted successfully."

    def add_ticker_to_portfolio(self, portfolio_name: str, ticker: str, quantity: int = 1) -> None:
        all_portfolios = self.get_all_portfolios()
        if portfolio_name not in all_portfolios:
            logger.error(f"Portfolio '{portfolio_name}' does not exist.")
            return
        portfolio = all_portfolios[portfolio_name]
        if not portfolio.is_editable:
            logger.error(f"Portfolio '{portfolio_name}' is not editable.")
            return
        # Check if ticker already exists
        for position in portfolio.positions:
            if position.ticker == ticker:
                # Ticker already exists
                # Warning and add quantity to position
                logger.warning(f"Ticker '{ticker}' already exists in portfolio '{portfolio_name}'.")
                position.shares = (position.shares or 0) + quantity
                self._dump_portfolios(all_portfolios)
                return

        # position does not exist, add new
        portfolio.positions.append(
            Position(
                ticker=ticker,
                shares=quantity,
            )
        )
        self._dump_portfolios(all_portfolios)

    def remove_ticker_from_portfolio(self, portfolio_name: str, ticker: str) -> None:
        all_portfolios = self.get_all_portfolios()
        if portfolio_name not in all_portfolios:
            logger.error(f"Portfolio '{portfolio_name}' does not exist.")
            return
        portfolio = all_portfolios[portfolio_name]
        original_count = len(portfolio.positions)
        portfolio.positions = [
            position for position in portfolio.positions if position.ticker != ticker
        ]
        if len(portfolio.positions) == original_count:
            logger.warning(f"Ticker '{ticker}' not found in portfolio '{portfolio_name}'.")
        self._dump_portfolios(all_portfolios)

    def update_position_share_count(
        self, portfolio_name: str, ticker: str, new_shares: float
    ) -> None:
        """Sets the absolute share count for a position."""
        if new_shares <= 0:
            logger.info(f"Shares for '{ticker}' set to {new_shares}. Removing position.")
            self.remove_ticker_from_portfolio(portfolio_name, ticker)
            return
        all_portfolios = self.get_all_portfolios()
        if portfolio_name not in all_portfolios:
            return

        portfolio = all_portfolios[portfolio_name]
        if not portfolio.is_editable:
            logger.error(f"Portfolio '{portfolio_name}' is read-only.")
            return

        found = False
        for pos in portfolio.positions:
            if pos.ticker == ticker:
                pos.shares = new_shares
                found = True
                break

        if found:
            self._dump_portfolios(all_portfolios)
            logger.info(f"Updated '{ticker}' in '{portfolio_name}' to {new_shares} shares.")


class AdminEngine:
    def __init__(self, config_path: Path | None = None) -> None:
        self.config: Config = load_config()

        self.metadata_storage = ParquetStorage(self.config.settings.metadata_dir)
        self.prices_storage = ParquetStorage(self.config.settings.prices_dir)
        self.fundamentals_storage = ParquetStorage(self.config.settings.fundamentals_dir)

        self.extractor = DataExtractor()
        self.metadata_pipeline = ETLPipeline(self.metadata_storage, self.extractor)
        self.prices_pipeline = ETLPipeline(self.prices_storage, self.extractor)
        self.fundamentals_pipeline = ETLPipeline(self.fundamentals_storage, self.extractor)

        config_dir = config_path if config_path else Path("config")
        self.portfolio_manager = UserPortfolioManager(config_dir)

    def _update_tickers(self, tickers: list[str]) -> None:
        """Update data for the given tickers."""
        try:
            self.metadata_pipeline.run_metadata_update(tickers=tickers)

            metadata_df = self.metadata_storage.read("asset_metadata")
            if metadata_df is None or metadata_df.is_empty():
                logger.warning(
                    "No metadata available after update. Skipping price/fundamental update."
                )
                return

            self.prices_pipeline.run_price_update(tickers=tickers, metadata=metadata_df)
            stock_tickers = set(
                metadata_df.filter(
                    (pl.col("asset_type") == AssetType.STOCK) & (pl.col("ticker").is_in(tickers))
                )["ticker"].to_list()
            )
            self.fundamentals_pipeline.run_fundamental_update(
                tickers=list(stock_tickers), metadata=metadata_df
            )
        except Exception as e:
            logger.error(f"Error updating tickers {tickers}: {e}")
            raise e

    def get_known_tickers(self) -> list[str]:
        """Get all known tickers from metadata storage."""
        df_meta = self.metadata_storage.read("asset_metadata")
        if df_meta is None or df_meta.is_empty():
            return []
        tickers = set(df_meta["ticker"].to_list())
        return sorted(tickers)

    def get_all_portfolios(self) -> dict[str, Portfolio]:
        """Get all portfolios managed by the portfolio manager."""
        return self.portfolio_manager.get_all_portfolios()

    def is_ticker_known(self, ticker: str) -> bool:
        """Check if a ticker is known in the metadata storage."""
        known_tickers = self.get_known_tickers()
        return ticker in known_tickers

    def init_new_ticker(self, ticker: str, force_reload: bool = False) -> None:
        """Initialize data for a new ticker by running the ETL pipeline."""
        if not force_reload and self.is_ticker_known(ticker):
            logger.info(f"Ticker '{ticker}' is already known. Skipping initialization.")
            return
        logger.info(f"Initializing data for new ticker '{ticker}'.")
        self._update_tickers(tickers=[ticker])

    def update_portfolio_data(self, portfolio_name: str) -> None:
        """Update data for all tickers in the given portfolio."""
        portfolios = self.portfolio_manager.get_all_portfolios()
        if portfolio_name not in portfolios:
            logger.error(f"Portfolio '{portfolio_name}' does not exist.")
            return
        portfolio = portfolios[portfolio_name]
        tickers = portfolio.tickers
        logger.info(f"Updating data for portfolio '{portfolio_name}' with tickers: {tickers}")
        self._update_tickers(tickers=tickers)
        logger.info(f"Data update for portfolio '{portfolio_name}' completed.")

    def update_all_portfolios(self) -> None:
        """Update data for all portfolios."""
        portfolios = self.portfolio_manager.get_all_portfolios()
        all_tickers = set()
        for portfolio in portfolios.values():
            all_tickers.update(portfolio.tickers)
        tickers = sorted(all_tickers)
        logger.info(f"Updating data for all portfolios with tickers: {tickers}")
        self._update_tickers(tickers=tickers)
        logger.info("Data update for all portfolios completed.")

    def make_snapshot(self) -> None:
        """Create a snapshot of the current data state."""
        make_snapshot()
