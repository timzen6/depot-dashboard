import polars as pl
import streamlit as st

from src.app.logic.common import get_sorted_occurrences
from src.app.logic.data_loader import DashboardData, load_all_stock_data
from src.app.logic.screener import prepare_screener_snapshot
from src.app.views.constants import (
    assign_info_emojis,
)
from src.app.views.screener import (
    render_factor_overview_chart,
    render_in_depth_performance_charts,
    render_info_table,
    render_sidebar_selection,
    render_stats_table,
)
from src.core.domain_models import AssetType
from src.core.strategy_engine import StrategyEngine

# Page config
st.set_page_config(
    page_title="Stock Detail",
    # other emojo than details and admin page
    page_icon="🚦",
    layout="wide",
)


dashboard_data, portfolio_dict, fx_engine = load_all_stock_data()

all_stock_metadata = dashboard_data.metadata.filter(
    pl.col("asset_type") == AssetType.STOCK.value
).pipe(assign_info_emojis, "sector", "country", "asset_type", "name")

all_sectors = get_sorted_occurrences(all_stock_metadata, "sector")
all_countries = get_sorted_occurrences(all_stock_metadata, "country")
strategy_engine = StrategyEngine()


portfolio_filter, sector_filter = render_sidebar_selection(portfolio_dict, all_sectors)

# Apply filters
filtered_metadata = all_stock_metadata

if portfolio_filter:
    selected_tickers = set()
    for pf in portfolio_filter:
        selected_tickers.update(portfolio_dict.get(pf, []))
    filtered_metadata = filtered_metadata.filter(pl.col("ticker").is_in(list(selected_tickers)))

if sector_filter:
    filtered_metadata = filtered_metadata.filter(pl.col("sector").is_in(sector_filter))


@st.fragment()  # type: ignore[misc]
def render_dashboard_content(
    filtered_metadata: pl.DataFrame,
    dashboard_data: DashboardData,
    strategy_engine: StrategyEngine,
) -> None:
    st.title("🚦 Stock Screener")
    col1, col2 = st.columns([3, 2])
    with col1:
        selected_tickers = render_info_table(filtered_metadata)
    with col2:
        render_factor_overview_chart(
            selected_tickers,
            filtered_metadata,
            strategy_engine,
        )

    df_prices_latest = prepare_screener_snapshot(
        dashboard_data.prices,
        dashboard_data.fundamentals,
        filtered_metadata,
        fx_engine,
        selected_tickers,
    )
    plot_selection = render_stats_table(df_prices_latest)

    if not plot_selection:
        st.info("No stocks selected for plotting.")
        return
    render_in_depth_performance_charts(
        plot_selection,
        dashboard_data,
    )


render_dashboard_content(filtered_metadata, dashboard_data, strategy_engine)
