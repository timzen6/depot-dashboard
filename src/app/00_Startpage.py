"""Quality Core Dashboard - Main Entry Point.

Welcome page for the Streamlit application.
Navigate to specific pages using the sidebar.
"""

import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.portfolio import PortfolioEngine
from src.app.logic.data_loader import GlobalDataLoader, load_all_stock_data
from src.app.logic.screener import prepare_screener_snapshot
from src.app.logic.startpage import (
    calculate_multiple_portfolio_metrics,
    check_price_alarms,
    check_watch_list,
)
from src.app.views.startpage import (
    render_info_section,
    render_portfolio_overview_table,
    render_price_alarms_section,
    render_recent_reports_section,
    render_stocks_to_watch_table,
    render_watch_list_alert_tables,
)
from src.config.landing_page import load_landing_page_config
from src.config.models import PortfolioType
from src.core.etf_loader import ETFLoader
from src.core.strategy_engine import StrategyEngine

st.set_page_config(
    page_title="Quality Core Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Header
st.title("📈 Quality Core Dashboard")
st.divider()

try:
    loader = GlobalDataLoader()
    data = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e

etf_loader = ETFLoader(loader.config.settings.etf_config_dir)
data, _, fx_engine = load_all_stock_data()
portfolios_config = loader.load_portfolios()

landing_config = load_landing_page_config()
portfolio_engine = PortfolioEngine()
strategy_engine = StrategyEngine()


try:
    tnx_data = data.prices.filter(pl.col("ticker") == "^TNX").sort("date").tail(1)
    if tnx_data.is_empty():
        treasury_yield = None
    else:
        treasury_yield = tnx_data.select(pl.col("close")).item()
except Exception as e:
    logger.error(f"Error fetching treasury yield: {e}", exc_info=True)
    treasury_yield = None

if portfolios_config is None:
    relevant_portfolios = {}
else:
    relevant_portfolios = {
        k: v
        for k, v in portfolios_config.items()
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

selected_ticker = landing_config.watchlist_tickers

col1, col2 = st.columns([5, 2])
with col1:
    subcol1, subcol2 = st.columns([3, 1])
    with subcol1:
        st.header("📁 Portfolio Overview")
    with subcol2:
        st.metric(label="📊 Current 10Y Treasury Yield", value=f"{treasury_yield:.1f}%")

    # leave some vertical space
    st.subheader("")
    render_portfolio_overview_table(df_portfolio)
with col2:
    st.header("📅 Recent Earnings")
    render_recent_reports_section(data, selected_ticker)

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

st.header("🔔 Stocks to Watch")
col1, col2 = st.columns([3, 2])
with col1:
    render_stocks_to_watch_table(df_screener_snapshot)
with col2:
    watch_list = [alert.model_dump() for alert in landing_config.alerts]
    df_watch = check_watch_list(
        data.prices.join(
            data.metadata.select("ticker", "name", "asset_type", "forward_pe"),
            on="ticker",
            how="left",
        ),
        watch_list,
        fx_engine,
    )
    render_watch_list_alert_tables(df_watch)

    df_price_alarms = check_price_alarms(data.prices, landing_config.price_alarms, fx_engine)
    subcol1, subcol2 = st.columns([3, 1])
    with subcol1:
        st.subheader("⏰ Price Alarms")
    with subcol2:
        display_all = st.toggle(
            "Show All",
            value=False,
            help="Toggle to show all price alarms or only triggered ones.",
        )
    render_price_alarms_section(df_price_alarms, display_all=display_all)


st.header("📖 Strategy Manifest and Factor Definitions")
render_info_section(landing_config)
