"""Stock Detail Page.

Wiring layer for stock-specific analysis with multiple tabs.
Shows valuation, quality, and financial health metrics.
"""

import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.app.logic.data_loader import GlobalDataLoader
from src.app.logic.stock_detail import (
    get_all_tickers,
)
from src.app.views.common import (
    portfolio_selection,
    render_empty_state,
    render_sidebar_header,
)
from src.app.views.stock_detail import (
    render_growth_data,
    render_health_data,
    render_latest_price_info,
    render_pe_ratio_chart,
    render_price_chart,
    render_quality_data,
    render_title_section,
    render_valuation_data,
)
from src.core.stock_data import StockData

# Page config
st.set_page_config(
    page_title="Stock Detail",
    page_icon="🔍",
    layout="wide",
)

# Sidebar
render_sidebar_header("Stock Detail", "Deep dive into individual stocks")

# Load data
try:
    loader = GlobalDataLoader()
    dashboard_data = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e
# Get available tickers
if dashboard_data.prices.is_empty():
    render_empty_state("No price data available")
    st.stop()

selected_portfolio = portfolio_selection(
    (list(loader.config.portfolios.portfolios.values()) if loader.config.portfolios else []),
    allow_none=True,
    on_sidebar=True,
)
tickers = selected_portfolio.tickers if selected_portfolio else get_all_tickers()
if not tickers:
    render_empty_state("No tickers found in dataset")
    st.stop()

selected_ticker = st.sidebar.selectbox(
    "Select Ticker",
    options=tickers,
    index=0,
)

# Filters
st.sidebar.divider()
st.sidebar.subheader("Filters")

# Date range filter
min_date = dashboard_data.prices.select("date").min().item()
max_date = dashboard_data.prices.select("date").max().item()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

try:
    # Load ticker data
    stock_data = StockData.from_dataset(selected_ticker, dashboard_data)
    fx_engine = FXEngine(dashboard_data.prices, target_currency="EUR")
    render_title_section(selected_ticker, stock_data.metadata)

    filtered_stock_data = stock_data.filter_date_range(
        start_date=date_range[0],
        end_date=date_range[1],
    )

    if filtered_stock_data.is_empty:
        render_empty_state(f"No data available for {selected_ticker}")
        st.stop()

    # Create tabs for different analyses
    st.subheader("📈 Price History")
    render_latest_price_info(filtered_stock_data.prices, fx_engine)
    tab1, tab2, tab3 = st.tabs(["Simple Chart", "Analyst Style Chart", "PE Ratio Chart"])
    with tab1:
        render_price_chart(
            filtered_stock_data.prices,
            selected_ticker,
            simple_display_mode=True,
        )
    with tab2:
        render_price_chart(
            filtered_stock_data.prices,
            selected_ticker,
            simple_display_mode=False,
        )
    with tab3:
        render_pe_ratio_chart(
            filtered_stock_data.prices,
            selected_ticker,
        )

    if filtered_stock_data.fundamentals.is_empty():
        st.info("No fundamental data available for this ticker")
        st.stop()
    high_tabs = st.tabs(["💰 Valuation", "💎 Quality", "🚀 Growth", "🏥 Health"])
    with high_tabs[0]:
        render_valuation_data(stock_data)
    with high_tabs[1]:
        render_quality_data(stock_data)
    with high_tabs[2]:
        render_growth_data(stock_data)
    with high_tabs[3]:
        render_health_data(stock_data)
except Exception as e:
    st.exception(e)
    logger.error(f"Stock detail error: {e}", exc_info=True)
