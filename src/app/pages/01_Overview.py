"""Portfolio Overview Page.

Wiring layer connecting overview logic and views.
Shows portfolio performance and current positions.
"""

import polars as pl
import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.analysis.portfolio import PortfolioEngine
from src.app.logic.data_loader import GlobalDataLoader
from src.app.logic.overview import (
    get_market_snapshot,
    get_portfolio_kpis,
    get_portfolio_performance,
)
from src.app.views.common import (
    portfolio_selection,
    render_empty_state,
    render_kpi_cards,
    render_sidebar_header,
)
from src.app.views.overview import (
    render_market_snapshot_table,
    render_portfolio_chart,
    render_portfolio_composition_chart,
    render_positions_table,
    render_stock_composition_chart,
)
from src.core.domain_models import AssetType

# Page config
st.set_page_config(
    page_title="Portfolio Overview",
    page_icon="📊",
    layout="wide",
)

# Sidebar
render_sidebar_header("Portfolio Overview", "Select a portfolio to analyze")

# Load data
try:
    loader = GlobalDataLoader()
    data = loader.load_data()
    df_prices = data.prices
    df_fund = data.fundamentals
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e

portfolios_config = loader.config.portfolios
# Get available portfolios
if portfolios_config is None:
    render_empty_state("No portfolios configured. Please add portfolios to portfolios.yaml")
    st.stop()
assert portfolios_config is not None  # Type narrowing for mypy

portfolios = portfolios_config.portfolios
if portfolios is None:
    st.error("No portfolios found in configuration")
    st.stop()
assert portfolios is not None  # Type narrowing for mypy


fx_engine = FXEngine(df_prices=df_prices, target_currency="EUR")
portfolio_engine = PortfolioEngine()

if not portfolios:
    render_empty_state("No portfolios configured. Please add portfolios to portfolios.yaml")
    st.stop()

selected_portfolio = portfolio_selection(portfolios=list(portfolios.values()), on_sidebar=True)
if selected_portfolio is None:
    st.error("No portfolio selected")
    st.stop()

assert selected_portfolio is not None  # Type narrowing for mypy

st.sidebar.info(
    f"**Type:** {selected_portfolio.type.value.title()}\n\n"
    f"**Positions:** {len(selected_portfolio.positions)}\n\n"
    f"**Start Date:** {selected_portfolio.start_date or 'N/A'}"
)

# Main content
st.title(f"📊 {selected_portfolio.ui_name}")

try:
    # Get portfolio performance
    df_history = get_portfolio_performance(
        selected_portfolio, df_prices, fx_engine, portfolio_engine
    ).join(data.metadata, on="ticker", how="left")

    if df_history.is_empty():
        render_empty_state(
            f"No price data available for {selected_portfolio.ui_name} positions",
            icon="⚠️",
        )
        st.stop()

    # Calculate KPIs
    st.dataframe(df_history)
    kpis = get_portfolio_kpis(df_history)

    # Render KPIs
    render_kpi_cards(kpis)

    st.divider()

    tab1, tab2 = st.tabs(["Complete Portfolio", "Stock Breakdown"])
    # Portfolio chart and composition
    with tab1:
        col1, col2 = st.columns([3, 2])
        with col1:
            render_portfolio_chart(df_history, key="portfolio_chart_all")

        with col2:
            render_portfolio_composition_chart(df_history)
    with tab2:
        df_history_stock = df_history.filter(pl.col("asset_type") == AssetType.STOCK)
        col1, col2 = st.columns([3, 2])
        with col1:
            render_portfolio_chart(df_history_stock, key="portfolio_chart_stocks")
        with col2:
            render_stock_composition_chart(df_history_stock)

    st.divider()

    with st.expander("Show Detailed Positions Table", expanded=True):
        render_positions_table(
            df_history, selected_portfolio.display_name or selected_portfolio.name
        )
    # Market snapshot
    snapshot = get_market_snapshot(data, fx_engine, selected_portfolio.tickers)

    st.subheader("📋 Market Fundamentals")
    render_market_snapshot_table(snapshot)

except Exception as e:
    st.error(f"Error calculating portfolio performance: {e}")
    logger.error(f"Portfolio calculation error: {e}", exc_info=True)
    raise e
