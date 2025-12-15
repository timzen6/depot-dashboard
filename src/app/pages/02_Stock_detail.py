"""Stock Detail Page.

Wiring layer for stock-specific analysis with multiple tabs.
Shows valuation, quality, and financial health metrics.
"""

import streamlit as st
from loguru import logger

from src.analysis.fx import FXEngine
from src.app.logic.data_loader import GlobalDataLoader
from src.app.logic.stock_detail import (
    filter_data_by_date_range,
    get_available_tickers,
    get_ticker_data,
    get_ticker_fundamentals,
)
from src.app.views.common import render_empty_state, render_sidebar_header
from src.app.views.stock_detail import (
    render_fundamental_chart,
    render_price_chart,
)
from src.app.views.stock_detail.charts import (
    render_latest_price_info,
    render_quality_metrics_data,
)

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
    df_prices, df_fund = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    st.stop()

# Get available tickers
if df_prices.is_empty():
    render_empty_state("No price data available")
    st.stop()


portfolio_name_dict = (
    {p.ui_name: p.name for p in loader.config.portfolios.portfolios.values()}
    if loader.config.portfolios
    else {}
)

# Ticker selector
selected_portfolio_name = st.sidebar.selectbox(
    "Select Portfolio (optional)",
    options=["All"] + list(portfolio_name_dict.keys()),
    index=0,
)
selected_portfolio = portfolio_name_dict.get(selected_portfolio_name, None)
tickers = get_available_tickers(selected_portfolio)
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
min_date = df_prices.select("date").min().item()
max_date = df_prices.select("date").max().item()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Main content
st.title(f"🔍 {selected_ticker}")

try:
    # Load ticker data
    df_ticker_prices = get_ticker_data(selected_ticker, df_prices)
    df_ticker_fund = get_ticker_fundamentals(selected_ticker, df_fund)
    fx_engine = FXEngine(df_prices, target_currency="EUR")

    try:
        result = filter_data_by_date_range(
            df_ticker_prices,
            df_ticker_fund,
            start_date=date_range[0],
            end_date=date_range[1],
        )
        df_ticker_prices = result[0]
        df_ticker_fund = result[1]
    except ValueError as ve:
        logger.error(f"Date range error: {ve}")

    if df_ticker_prices.is_empty():
        render_empty_state(f"No data available for {selected_ticker}")
        st.stop()

    # Create tabs for different analyses
    st.subheader("Price History")
    render_latest_price_info(df_ticker_prices, selected_ticker, fx_engine)
    tab1, tab2 = st.tabs(["Simple Chart", "Analyst Style Chart"])
    with tab1:
        render_price_chart(
            df_ticker_prices,
            selected_ticker,
            simple_display_mode=True,
        )
    with tab2:
        render_price_chart(
            df_ticker_prices,
            selected_ticker,
            simple_display_mode=False,
        )

    # Show latest price info
    render_quality_metrics_data(df_ticker_fund, df_ticker_prices)

    st.subheader("Quality Metrics")
    if df_ticker_fund.is_empty():
        st.info("No fundamental data available for this ticker")
    else:
        render_fundamental_chart(df_ticker_fund, selected_ticker)

except Exception as e:
    st.error(f"Error loading stock data: {e}")
    logger.error(f"Stock detail error: {e}", exc_info=True)
