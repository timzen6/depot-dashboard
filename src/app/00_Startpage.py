"""Quality Core Dashboard - Main Entry Point.

Welcome page for the Streamlit application.
Navigate to specific pages using the sidebar.
"""

from datetime import date, timedelta

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
    render_info_section,
    render_portfolio_overview_table,
    render_stocks_to_watch_table,
    render_watch_list_alert_tables,
)
from src.config.landing_page import load_landing_page_config
from src.config.models import PortfolioType
from src.core.domain_models import ReportType
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

landing_config = load_landing_page_config()
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

selected_ticker = landing_config.watchlist_tickers
watch_list = [alert.model_dump() for alert in landing_config.alerts]

col1, col2 = st.columns([5, 2])
with col1:
    st.header("📁 Portfolio Overview")
    # leave some vertical space
    st.subheader("")
    render_portfolio_overview_table(df_portfolio)
with col2:
    # TODO: Encapsulate in function
    st.header("📅 Recent Earnings")
    tmp_meta = (
        data.metadata.filter(pl.col("ticker").is_in(selected_ticker))
        .select(["ticker", "display_name", "short_name", "earnings_date", "dividend_date"])
        .with_columns(pl.coalesce(pl.col("display_name"), pl.col("short_name")).alias("name"))
        .drop("short_name", "display_name")
    )
    tmp_fund = (
        (
            data.fundamentals.filter(pl.col("ticker").is_in(selected_ticker))
            .select(["ticker", "date", "period_type"])
            .sort(["ticker", "date"], descending=False)
        )
        .filter(pl.col("period_type") == ReportType.ANNUAL)
        .group_by("ticker")
        .agg(pl.col("date").last())
        .with_columns(
            # eastimated next fiscal year end by adding 1 year
            (pl.col("date") + pl.duration(days=365)).alias("est_next_annual_earning")
        )
        .rename({"date": "last_annual_earning"})
    )

    tmp = tmp_meta.join(tmp_fund, on="ticker", how="left").sort(
        ["est_next_annual_earning", "ticker"]
    )
    # for now we take the estimated next annual earning to
    # have annual report alerts consistently
    # (quarterly reports are not always available and less relevant in general)
    today = date.today()
    end_lookup = today + timedelta(days=30)
    tmp_next_earnings = tmp.filter(
        (pl.col("est_next_annual_earning") >= today)
        & (pl.col("est_next_annual_earning") <= end_lookup)
    )
    if tmp_next_earnings.is_empty():
        st.info("No upcoming earnings dates in the next 30 days.")
    else:
        st.subheader("Upcoming Earnings Dates")
        st.dataframe(
            tmp_next_earnings,
            column_order=["ticker", "name", "est_next_annual_earning"],
            column_config={
                "est_next_annual_earning": st.column_config.DateColumn(
                    "Next Annual Earnings",
                    format="YYYY-MM-DD",
                ),
                "ticker": "Ticker",
                "name": "Company Name",
            },
        )
    tmp_recent_earnings = tmp.filter(
        pl.col("last_annual_earning") >= today - timedelta(days=60)
    ).sort(["last_annual_earning", "ticker"], descending=True)
    if tmp_recent_earnings.is_empty():
        st.info("No recent earnings reports.")
    else:
        st.subheader("Recently Reported Earnings")
        st.dataframe(
            tmp_recent_earnings,
            column_order=["ticker", "name", "last_annual_earning"],
            column_config={
                "last_annual_earning": st.column_config.DateColumn(
                    "Last Annual Earnings",
                    format="YYYY-MM-DD",
                ),
                "ticker": "Ticker",
                "name": "Company Name",
            },
        )


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

st.header("📖 Strategy Manifest and Factor Definitions")
render_info_section(landing_config)
