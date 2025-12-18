"""Stock detail logic package.

Exports main data loading functions for stock-specific analysis.
"""

from src.config.settings import load_config

config = load_config()


def get_all_tickers() -> list[str]:
    if config.portfolios is None:
        return []
    return list(config.portfolios.all_tickers)
