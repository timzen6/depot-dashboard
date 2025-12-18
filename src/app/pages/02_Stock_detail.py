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
    df_prices, df_fund = loader.load_data()
except Exception as e:
    st.error(f"Failed to load data: {e}")
    logger.error(f"Data loading error: {e}", exc_info=True)
    raise e
# Get available tickers
if df_prices.is_empty():
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
    stock_data = StockData.from_dataset(selected_ticker, df_prices, df_fund)
    fx_engine = FXEngine(df_prices, target_currency="EUR")

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

    # Valuation Metrics
    render_valuation_data(stock_data)
    # Quality
    render_quality_data(stock_data)
    render_growth_data(stock_data)
    render_health_data(stock_data)
except Exception as e:
    st.error(f"Error loading stock data: {e}")
    logger.error(f"Stock detail error: {e}", exc_info=True)
