"""Portfolio Overview Page.

Wiring layer connecting overview logic and views.
Shows portfolio performance and current positions.
"""

import streamlit as st
from loguru import logger

from src.app.logic.data_loader import GlobalDataLoader
from src.app.logic.overview import (
    get_all_portfolios,
    get_portfolio_kpis,
    get_portfolio_performance,
)
from src.app.views.common import (
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
    st.stop()

# Get available portfolios
portfolios = get_all_portfolios()

if not portfolios:
    render_empty_state("No portfolios configured. Please add portfolios to portfolios.yaml")
    st.stop()

# Portfolio selector
# Build mapping: UI name -> portfolio ID
ui_name_to_id = {p.ui_name: pid for pid, p in portfolios.items()}
ui_names = list(ui_name_to_id.keys())

selected_ui_name = st.sidebar.selectbox(
    "Select Portfolio",
    options=ui_names,
    index=0,
)

# Get portfolio ID from UI name selection
selected_portfolio_id = ui_name_to_id[selected_ui_name]


# Display portfolio info
selected = portfolios.get(selected_portfolio_id)
if selected is None:
    st.error(f"Portfolio '{selected_portfolio_id}' not found")
    st.stop()

assert selected is not None  # Type narrowing for mypy

st.sidebar.info(
    f"**Type:** {selected.type.value.title()}\n\n"
    f"**Positions:** {len(selected.positions)}\n\n"
    f"**Start Date:** {selected.start_date or 'N/A'}"
)

# Main content
st.title(f"📊 {selected.ui_name}")

try:
    # Get portfolio performance
    df_history = get_portfolio_performance(selected_portfolio_id, df_prices)

    if df_history.is_empty():
        render_empty_state(
            f"No price data available for {selected.ui_name} positions",
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
    render_positions_table(df_history, selected_ui_name)

except Exception as e:
    st.error(f"Error calculating portfolio performance: {e}")
    logger.error(f"Portfolio calculation error: {e}", exc_info=True)
    raise e
