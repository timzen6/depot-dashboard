"""Stock Detail Page.

Wiring layer for stock-specific analysis with multiple tabs.
Shows valuation, quality, and financial health metrics.
"""

from datetime import datetime, timedelta

import polars as pl
import streamlit as st
from loguru import logger

from src.app.logic.common import get_sorted_occurrences
from src.app.logic.data_loader import GlobalDataLoader, load_all_stock_data
from src.app.logic.stock_detail import (
    get_all_tickers,
)
from src.app.views.common import (
    portfolio_selection,
    render_empty_state,
    render_sidebar_header,
)
from src.app.views.stock_detail import (
    render_etf_composition_charts,
    render_fcf_yield_chart,
    render_fundamentals_reference,
    render_growth_data,
    render_health_data,
    render_latest_price_info,
    render_pe_ratio_chart,
    render_price_chart,
    render_quality_data,
    render_title_section,
    render_valuation_data,
)
from src.core.domain_models import AssetType
from src.core.etf_loader import ETFLoader
from src.core.stock_data import StockData
from src.core.strategy_engine import StrategyEngine

# Page config
st.set_page_config(
    page_title="Stock Detail",
    page_icon="🔍",
    layout="wide",
)

# Sidebar
render_sidebar_header("Stock Detail", "Deep dive into individual stocks")
loader = GlobalDataLoader()
config = loader.config

portfolios = loader.load_portfolios()

dashboard_data, _, fx_engine = load_all_stock_data()

if dashboard_data.prices.is_empty():
    render_empty_state("No price data available")
    st.stop()

all_sectors = get_sorted_occurrences(dashboard_data.metadata, "sector")
selection_mode = st.sidebar.pills(
    "Selection Mode",
    options=["Portfolio", "Sector"],
    width=300,
    default="Portfolio",
)

if selection_mode == "Portfolio":  # Portfolio mode
    selected_portfolio = portfolio_selection(
        (list(portfolios.values()) if portfolios else []),
        allow_none=True,
        on_sidebar=True,
    )
    tickers = selected_portfolio.tickers if selected_portfolio else get_all_tickers()
else:  # Sector mode
    selected_sector = st.sidebar.selectbox(
        "Select Sector",
        options=["All Sectors"] + all_sectors,
        index=0,
    )
    if selected_sector == "All Sectors":
        tickers = get_all_tickers()
    else:
        df_filtered = dashboard_data.metadata.filter(pl.col("sector") == selected_sector)
        tickers = df_filtered.select("ticker").to_series().to_list()

if not tickers:
    render_empty_state("No tickers found in dataset")
    st.stop()


selected_ticker = st.sidebar.selectbox(
    "Select Ticker",
    options=sorted(tickers),
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
    strategy_engine = StrategyEngine()
    etf_loader = ETFLoader(config.settings.etf_config_dir)

    filtered_stock_data = stock_data.filter_date_range(
        start_date=date_range[0],
        end_date=date_range[1],
    )
    data_source_info = (
        filtered_stock_data.prices.sort("date")
        .tail(1)
        .select(["date", "valuation_source", "data_lag_days"])
    )
    valuation_source = data_source_info.select("valuation_source").item()
    data_lag_days = data_source_info.select("data_lag_days").item()

    render_title_section(
        selected_ticker,
        stock_data.metadata,
        strategy_engine,
        valuation_source,
        data_lag_days,
    )

    if filtered_stock_data.is_empty:
        render_empty_state(f"No data available for {selected_ticker}")
        st.stop()

    # Create tabs for different analyses
    st.subheader("📈 Price History")
    render_latest_price_info(filtered_stock_data.prices, fx_engine)

    time_delta_selection = st.pills(
        "Select Time Displayed",
        options=["1M", "3M", "6M", "YTD", "Max"],
    )
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Simple Chart",
            "Simple Chart (EUR)",
            "PE Ratio Chart",
            "FCF Yield vs. Price Chart",
            "Analyst Style Chart",
        ]
    )

    time_delta_mapping = {
        "1M": 1,
        "3M": 3,
        "6M": 6,
        "YTD": 12,
        "Max": None,
    }

    now = datetime.now().date()
    time_delta = time_delta_mapping.get(time_delta_selection, 12)

    if time_delta_selection == "Max" or time_delta_selection is None or time_delta is None:
        start_date = None
    else:
        start_date = now - timedelta(days=time_delta * 31)
    with tab1:
        render_price_chart(
            filtered_stock_data.prices,
            selected_ticker,
            simple_display_mode=True,
            fx_engine=fx_engine,
            use_euro=False,
            start_date=start_date,
        )
    with tab2:
        render_price_chart(
            filtered_stock_data.prices,
            selected_ticker,
            simple_display_mode=True,
            fx_engine=fx_engine,
            use_euro=True,
            start_date=start_date,
        )

    with tab3:
        render_pe_ratio_chart(
            filtered_stock_data.prices,
            selected_ticker,
            start_date=start_date,
        )
    with tab4:
        render_fcf_yield_chart(
            filtered_stock_data.prices,
            selected_ticker,
            fx_engine,
            start_date=start_date,
            use_log=True,
        )
    with tab5:
        render_price_chart(
            filtered_stock_data.prices,
            selected_ticker,
            simple_display_mode=False,
            fx_engine=fx_engine,
            start_date=start_date,
        )

    asset_type = stock_data.metadata.get("asset_type")
    etf_data = etf_loader.get(selected_ticker)
    if asset_type == AssetType.ETF and etf_data is not None:
        render_etf_composition_charts(etf_data, strategy_engine)

    if filtered_stock_data.fundamentals.is_empty():
        st.info("No fundamental data available for this ticker")
        st.stop()
    high_tabs = st.tabs(["💰 Valuation", "💎 Quality", "🚀 Growth", "🏥 Health"])
    with high_tabs[0]:
        render_valuation_data(stock_data, fx_engine)
    with high_tabs[1]:
        render_quality_data(stock_data, fx_engine)
    with high_tabs[2]:
        render_growth_data(stock_data)
    with high_tabs[3]:
        render_health_data(stock_data)
    render_fundamentals_reference(stock_data.fundamentals)
except Exception as e:
    st.exception(e)
    logger.error(f"Stock detail error: {e}", exc_info=True)
