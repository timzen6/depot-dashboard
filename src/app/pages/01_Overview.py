"""Portfolio Overview Page.

Wiring layer connecting overview logic and views.
Shows portfolio performance and current positions.
"""

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
    render_portfolio_chart,
    render_portfolio_composition_chart,
    render_positions_table,
)

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
    df_prices, df_fund = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e

# Get available portfolios
if loader.config.portfolios is None:
    render_empty_state("No portfolios configured. Please add portfolios to portfolios.yaml")
    st.stop()

assert loader.config.portfolios is not None  # Type narrowing for mypy

portfolios = loader.config.portfolios.portfolios
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
    )

    if df_history.is_empty():
        render_empty_state(
            f"No price data available for {selected_portfolio.ui_name} positions",
            icon="⚠️",
        )
        st.stop()

    # Calculate KPIs
    kpis = get_portfolio_kpis(df_history)

    # Render KPIs
    render_kpi_cards(kpis)

    st.divider()

    # Portfolio chart and composition
    col1, col2 = st.columns([2, 1])

    with col1:
        render_portfolio_chart(df_history)

    with col2:
        render_portfolio_composition_chart(df_history)

    st.divider()

    # Positions table
    render_positions_table(df_history, selected_portfolio.display_name or selected_portfolio.name)
    # Market snapshot
    snapshot = get_market_snapshot(df_prices, df_fund, selected_portfolio.tickers)
    st.subheader("📋 Market Fundamentals")
    st.dataframe(
        snapshot,
        column_order=[
            "ticker",
            "market_cap_b_eur",
            "pe_ratio",
            "fcf_yield",
            "roce",
            "gross_margin",
            "revenue_growth",
            "net_debt_to_ebit",
        ],
        column_config={
            "market_cap_b_eur": st.column_config.NumberColumn("Market Cap 💶", format="%.1f B€"),
            "pe_ratio": st.column_config.NumberColumn("P/E 💰", format="%.1f"),
            "fcf_yield": st.column_config.NumberColumn("FCF Yield 💰", format="%.2f%%"),
            "gross_margin": st.column_config.NumberColumn("Gross Margin 💎", format="%.1f%%"),
            "roce": st.column_config.NumberColumn("ROCE 💎", format="%.1f%%"),
            "revenue_growth": st.column_config.NumberColumn("Revenue Growth 🚀", format="%.1f%%"),
            "net_debt_to_ebit": st.column_config.NumberColumn("🏥 Debt/EBIT 🏥", format="%.1fx"),
        },
    )

except Exception as e:
    st.error(f"Error calculating portfolio performance: {e}")
    logger.error(f"Portfolio calculation error: {e}", exc_info=True)
    raise e
