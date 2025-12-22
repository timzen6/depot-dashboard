import pandas as pd
import plotly.express as px
import polars as pl
import streamlit as st

from src.app.logic.data_loader import DashboardData
from src.app.views.colors import COLOR_SCALE_CONTRAST, COLOR_SCALE_GREEN_RED, Colors
from src.app.views.constants import CURRENCY_SYMBOLS


def render_sidebar_selection(
    portfolio_dict: dict[str, list[str]], all_sectors: list[str]
) -> tuple[list[str], list[str]]:
    selection_defaults = {
        "portfolio_filter": [
            "Quality Core Holdings",
            "Watchlist Level 1",
        ],
        "sector_filter": [],
    }
    for key, default in selection_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    portfolio_filter = st.sidebar.multiselect(
        "Select portfolios",
        options=list(portfolio_dict.keys()),
        help="Filter stocks by portfolios. If none selected, all stocks are shown.",
        key="portfolio_filter",
    )
    sector_filter = st.sidebar.multiselect(
        "Select sectors",
        options=all_sectors,
        help="Filter stocks by sectors. If none selected, all stocks are shown.",
        key="sector_filter",
    )

    st.sidebar.button(
        "Clear Filters",
        on_click=lambda: st.session_state.update(selection_defaults),
        type="primary",
    )
    return portfolio_filter, sector_filter


# Helpers to color cells based on value
def color_pe_rank(val: float) -> str:
    if pd.isna(val):
        return ""
    color = COLOR_SCALE_GREEN_RED[4]
    val = val * 100  # convert to percentage
    if val < 25:
        color = COLOR_SCALE_GREEN_RED[0]  # Green
    elif val < 40:
        color = COLOR_SCALE_GREEN_RED[1]
    elif val < 60:
        color = COLOR_SCALE_GREEN_RED[2]
    elif val < 75:
        color = COLOR_SCALE_GREEN_RED[3]

    return f"background-color: {color}; color: white"


def color_peg_warning(val: float) -> str:
    """Calm colors for PEG ratio only warn if over 2.0"""
    return (
        "" if pd.isna(val) or val <= 2.5 else f"background-color: {Colors.light_red}; color: white"
    )


def color_debt_to_ebit_warning(val: float) -> str:
    """Calm colors for Debt to EBIT only warn if over 5.0"""
    return "" if pd.isna(val) or val <= 3.0 else f"background-color: {Colors.red}; color: white"


def key_to_selected_tickers(
    key_name: str, filtered_metadata: pl.DataFrame, return_all_if_none: bool = True
) -> list[str]:
    selected_rows = st.session_state.get(key_name, {}).get("selection", {}).get("rows", [])

    selected_tickers = (
        filtered_metadata.select(pl.col("ticker").gather(selected_rows)).to_series().to_list()
    )

    if not selected_tickers and return_all_if_none:
        selected_tickers = filtered_metadata.select(pl.col("ticker")).to_series().to_list()
    return selected_tickers


def render_info_table(filtered_metadata: pl.DataFrame) -> list[str]:
    """Render info table above screener table"""
    tmp_df = filtered_metadata.with_columns(
        pl.col("currency").map_elements(lambda x: CURRENCY_SYMBOLS.get(x, x)).alias("currency"),
    )
    st.dataframe(
        tmp_df,
        column_order=[
            "name",
            "ticker",
            "currency",
            "info",
            "sector",
            "industry",
        ],
        column_config={
            "name": st.column_config.TextColumn("Name", width="medium"),
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "currency": st.column_config.TextColumn("Currency", width="small"),
            "info": st.column_config.TextColumn("Info", width="small"),
            "sector": st.column_config.TextColumn("Sector", width="medium"),
            "industry": st.column_config.TextColumn("Industry", width="medium"),
        },
        selection_mode="multi-row",
        key="stock_screener_table",
        on_select="rerun",
        hide_index=True,
        use_container_width=False,
    )
    return key_to_selected_tickers("stock_screener_table", filtered_metadata)


def render_stats_table(df_prices_latest: pl.DataFrame) -> list[str]:
    """Render table with in-depth analysis statistics"""
    df_prices_pandas = df_prices_latest.with_columns(
        # convert percentages
        (pl.col("upside") * 100).round(2).alias("upside"),
        (pl.col("revenue_growth") * 100).round(2).alias("revenue_growth"),
        (pl.col("fcf_yield") * 100).round(2).alias("fcf_yield"),
        (pl.col("ebit_margin") * 100).round(2).alias("ebit_margin"),
        (pl.col("roce") * 100).round(2).alias("roce"),
    ).to_pandas()
    styler = (
        df_prices_pandas.style.apply(
            lambda _: df_prices_pandas["pe_rank"].map(color_pe_rank),
            subset=["pe_ratio"],
        )
        .apply(
            lambda _: df_prices_pandas["peg_ratio"].map(color_peg_warning),
            subset=["peg_ratio"],
        )
        .apply(
            lambda _: df_prices_pandas["net_debt_to_ebit"].map(color_debt_to_ebit_warning),
            subset=["net_debt_to_ebit"],
        )
    )

    st.dataframe(
        styler,
        hide_index=True,
        selection_mode="multi-row",
        key="plot_selection_table",
        on_select="rerun",
        column_order=[
            "name",
            "close",
            "upside",
            "pe_ratio",
            "pe_ratio_median",
            "fcf_yield",
            # We can put this in optionally later
            # "peg_ratio",
            "roce",
            "ebit_margin",
            "net_debt_to_ebit",
            "revenue_growth",
            "close_30d",
        ],
        column_config={
            "name": st.column_config.TextColumn("Name", width="medium"),
            "close_30d": st.column_config.LineChartColumn(
                "📈 30d Price Chart", width="medium", color="auto"
            ),
            "upside": st.column_config.ProgressColumn(
                "💰 Upside (Fair Value)",
                min_value=-50,
                max_value=50,
                format="%.0f%%",
                color="auto",
            ),
            "roce": st.column_config.ProgressColumn(
                "💎 ROCE", min_value=0, max_value=30, format="%.0f%%", color="auto"
            ),
            "ebit_margin": st.column_config.ProgressColumn(
                "💎 EBIT Margin",
                min_value=0,
                max_value=30,
                format="%.0f%%",
                color="auto",
            ),
            "fcf_yield": st.column_config.NumberColumn("💰 FCF Yield", format="%.1f%%"),
            "net_debt_to_ebit": st.column_config.NumberColumn("🏥 Net Debt to EBIT", format="%.1f"),
            "pe_ratio": st.column_config.NumberColumn("💰 P/E Ratio", format="%.1f"),
            "peg_ratio": st.column_config.NumberColumn("💰 PEG Ratio", format="%.1f"),
            "pe_ratio_median": st.column_config.NumberColumn("📊 P/E Median", format="%.1f"),
            "close": st.column_config.NumberColumn("💶 Price (EUR)", format="%.2f"),
            "revenue_growth": st.column_config.NumberColumn("🚀 Revenue Growth", format="%.0f%%"),
        },
    )
    return key_to_selected_tickers(
        "plot_selection_table", df_prices_latest, return_all_if_none=False
    )


def render_in_depth_performance_charts(
    plot_selection: list[str],
    dashboard_data: DashboardData,
    plot_height: int = 400,
) -> None:
    """Render in-depth performance charts for selected stocks."""
    df_filtered_prices = (
        dashboard_data.prices.filter(pl.col("ticker").is_in(plot_selection))
        .sort(["ticker", "date"])
        .join(dashboard_data.metadata.select(["ticker", "name"]), on="ticker", how="left")
        .drop_nulls(subset=["pe_ratio"])
        # cap long names
        .with_columns(
            pl.when(pl.col("name").str.len_chars() > 20)
            .then(pl.col("name").str.slice(0, 17) + "...")
            .otherwise(pl.col("name"))
            .alias("name")
        )
    )
    df_filtered_fundamentals = (
        dashboard_data.fundamentals.filter(pl.col("ticker").is_in(plot_selection))
        .sort(["ticker", "date"])
        .join(dashboard_data.metadata.select(["ticker", "name"]), on="ticker", how="left")
        # drop is roce and fcf yield and revenue growth is null at the same time
        .filter(~(pl.col("roce").is_null() & pl.col("revenue_growth").is_null()))
        # cap long names
        .with_columns(
            pl.when(pl.col("name").str.len_chars() > 20)
            .then(pl.col("name").str.slice(0, 17) + "...")
            .otherwise(pl.col("name"))
            .alias("name")
        )
    )

    col1, col2 = st.columns(2)

    with col1:
        fig_pe = px.line(
            df_filtered_prices,
            x="date",
            y="pe_ratio",
            color="name",
            title="P/E Ratio",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
            height=plot_height,
        )
        fig_pe.update_layout(legend=dict(title=""))
        st.plotly_chart(fig_pe, use_container_width=True)
    with col2:
        fig_roce = px.scatter(
            df_filtered_fundamentals,
            x="date",
            y="roce",
            color="name",
            title="ROCE",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
            height=plot_height,
        )
        fig_roce.update_layout(legend=dict(title=""))
        # add markers and lines
        # increase marker size
        fig_roce.update_traces(mode="markers+lines", marker=dict(size=12))
        st.plotly_chart(fig_roce, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        fig_fcf = px.line(
            df_filtered_prices,
            x="date",
            y="fcf_yield",
            color="name",
            title="FCF Yield",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
            height=plot_height,
        )
        fig_fcf.update_layout(legend=dict(title=""))
        st.plotly_chart(fig_fcf, use_container_width=True)
    with col4:
        fig_rev = px.scatter(
            df_filtered_fundamentals.drop_nulls(subset=["revenue_growth"]),
            x="date",
            y="revenue_growth",
            color="name",
            title="Revenue Growth",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
            height=plot_height,
        )
        fig_rev.update_layout(legend=dict(title=""))
        fig_rev.update_traces(mode="markers+lines", marker=dict(size=12))
        st.plotly_chart(fig_rev, use_container_width=True)
