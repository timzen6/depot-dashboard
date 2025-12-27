"""Quality Core Dashboard - Main Entry Point.

Welcome page for the Streamlit application.
Navigate to specific pages using the sidebar.
"""

import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.analysis.portfolio import PortfolioEngine
from src.app.logic.data_loader import GlobalDataLoader
from src.app.logic.screener import prepare_screener_snapshot
from src.app.logic.startpage import (
    calculate_multiple_portfolio_metrics,
    check_watch_list,
)
from src.app.views.startpage import (
    render_portfolio_overview_table,
    render_stocks_to_watch_table,
    render_watch_list_alert_tables,
)
from src.config.models import PortfolioType
from src.core.etf_loader import ETFLoader
from src.core.strategy_engine import StrategyEngine

st.set_page_config(
    page_title="Quality Core Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Header
st.title("📊 Quality Core Dashboard")
st.divider()

try:
    loader = GlobalDataLoader()
    data = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e

portfolios_config = loader.config.portfolios
fx_engine = FXEngine(data.prices)
portfolio_engine = PortfolioEngine()
etf_loader = ETFLoader(loader.config.settings.etf_config_dir)
strategy_engine = StrategyEngine()

if portfolios_config is None:
    relevant_portfolios = {}
else:
    relevant_portfolios = {
        k: v
        for k, v in portfolios_config.portfolios.items()
        if (v is not None) and (v.type == PortfolioType.ABSOLUTE)
    }
df_portfolio = calculate_multiple_portfolio_metrics(
    data,
    list(relevant_portfolios.values()),
    fx_engine,
    portfolio_engine,
    strategy_engine,
    etf_loader,
)

render_portfolio_overview_table(df_portfolio)


selected_ticker = [
    "MSFT",
    "AAPL",
    "AMZN",
    "ASML.AS",
    "AI.PA",
    "SU.PA",
    "ATCO-A.ST",
    "MC.PA",
    "RMS.PA",
    # Imideate watchlist
    "SPGI",
    "MA",
    "V",
    "ROG.SW",
    "NOVO-B.CO",
]

# selected_metadata = data.metadata.filter(pl.col("ticker").is_in(selected_ticker))

watch_list = [
    dict(ticker="MSFT", action="buy", metric="upside", threshold=20),
    dict(ticker="NOVO-B.CO", action="buy", metric="price", threshold=40),
    dict(ticker="SPGI", action="buy", metric="pe_ratio", threshold=35),
    dict(ticker="RMS.PA", action="buy", metric="price", threshold=2000),
    dict(ticker="ATCO-A.ST", action="buy", metric="pe_ratio", threshold=24),
    dict(ticker="AI.PA", action="buy", metric="upside", threshold=15),
    # Some Testing Dummies
    dict(ticker="LISP.SW", action="buy", metric="upside", threshold=5),
    dict(ticker="EL.PA", action="buy", metric="price", threshold=280),
    # Sell Positions
    dict(ticker="UNA.AS", action="sell", metric="upside", threshold=-25),
    # Some dummies for testing
    dict(ticker="AAPL", action="sell", metric="price", threshold=230),
    dict(ticker="AMZN", action="sell", metric="pe_ratio", threshold=30),
]

df_screener_snapshot = (
    prepare_screener_snapshot(
        data.prices,
        data.fundamentals,
        data.metadata,
        fx_engine,
        selected_ticker,
    )
    .join(data.metadata, on="ticker", how="left")
    .filter(pl.col("ticker").is_in(selected_ticker))
    .with_columns(pl.col("ticker").cast(pl.Enum(selected_ticker)).alias("ticker_enum"))
    .sort("ticker_enum")
)

col1, col2 = st.columns([3, 2])
with col1:
    render_stocks_to_watch_table(df_screener_snapshot)
with col2:
    df_watch = check_watch_list(
        data.prices.join(
            data.metadata.select("ticker", "name", "asset_type"),
            on="ticker",
            how="left",
        ),
        watch_list,
        fx_engine,
    )
    render_watch_list_alert_tables(df_watch)
