"""View components for portfolio overview page.

Renders portfolio performance charts and position tables.
"""

import plotly.express as px
import polars as pl
import streamlit as st
from views.colors import COLOR_SCALE_CONTRAST, Colors

from src.app.logic.overview import filter_days_with_incomplete_tickers
from src.app.views.constants import COUNTRY_FLAGS, SECTOR_EMOJI
from src.core.domain_models import AssetType


def render_portfolio_chart(
    df_history: pl.DataFrame,
    key: str = "portfolio_chart",
    group_column: str | None = None,
) -> None:
    """Render portfolio value over time as interactive line chart.

    Args:
        df_history: Portfolio history with columns [date, total_value]
        key: Unique key for the chart element
    """
    if df_history.is_empty():
        st.warning("No portfolio history data to display")
        return

    time_select = st.pills(
        "Select Time Range",
        options=["All", "1Y", "6M", "1M"],
        default="All",
        key=f"{key}_time_select",
        width=500,
    )
    timedelta_map = {
        "1Y": 365,
        "6M": 182,
        "1M": 30,
    }

    gr_cols = ["date"]
    use_group = False
    if group_column and group_column in df_history.columns:
        gr_cols.append(group_column)
        use_group = True

    df_plot = (
        df_history.pipe(filter_days_with_incomplete_tickers)
        .group_by(gr_cols)
        .agg(pl.sum("position_value_EUR").alias("total_value"))
    )

    if "asset_type" in df_plot.columns:
        df_plot = df_plot.with_columns(
            pl.col("asset_type").str.to_uppercase(),
        ).sort("date")
    if time_select != "All":
        df_history_date_max = df_plot.select(pl.max("date")).item()
        days_delta = timedelta_map[time_select]
        df_plot = df_plot.filter(
            pl.col("date") >= (df_history_date_max - pl.duration(days=days_delta))
        )

    if use_group:
        fig = px.area(
            df_plot,
            x="date",
            y="total_value",
            color=group_column,
            title="Portfolio Value Over Time by Group",
            labels={"total_value": "Portfolio Value (€)", "date": "Date"},
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        st.plotly_chart(fig, use_container_width=True, key=key)
        return

    # Create line chart
    fig = px.line(
        df_plot,
        x="date",
        y="total_value",
        title="Portfolio Value Over Time",
        labels={"total_value": "Portfolio Value (€)", "date": "Date"},
    )

    fig.update_layout(
        hovermode="x unified",
        template="plotly_white",
        height=500,
    )

    fig.update_traces(
        line=dict(width=2, color=Colors.blue),
        fill="tozeroy",
        fillcolor="rgba(31, 119, 180, 0.1)",
    )

    st.plotly_chart(fig, use_container_width=True, key=key)


def render_positions_table(df_history: pl.DataFrame, portfolio_name: str) -> None:
    """Render current portfolio positions as an interactive table.

    Shows latest position values and weights for each ticker.

    Args:
        df_history: Portfolio history with ticker-level positions
        portfolio_name: Portfolio identifier for display
    """
    if df_history.is_empty():
        st.warning("No position data to display")
        return

    st.subheader(f"Current Positions - {portfolio_name}")

    # Get latest date positions
    latest_date = df_history.select(pl.max("date")).item()

    df_current = df_history.filter(pl.col("date") == latest_date).select(
        ["ticker", "position_value", "currency", "short_name", "asset_type", "sector"]
    )
    df_current = (
        df_history.sort("date")
        .group_by("ticker")
        .agg(
            pl.last("position_value").alias("position_value"),
            pl.last("currency").alias("currency"),
            pl.last("short_name").alias("short_name"),
            pl.last("asset_type").str.to_uppercase().alias("asset_type"),
            pl.last("group").alias("group"),
            pl.last("sector").alias("sector"),
            pl.last("position_value_EUR").alias("position_value_EUR"),
            pl.last("position_dividend_yoy_EUR").alias("position_dividend_yoy_EUR"),
        )
    )

    # Calculate weights
    total_value = df_current.select(pl.sum("position_value_EUR")).item()

    df_display = df_current.with_columns(
        (pl.col("position_value_EUR") / total_value * 100).alias("weight_pct")
    ).sort("position_value_EUR", descending=True)

    st.dataframe(
        df_display,
        column_order=[
            "short_name",
            "ticker",
            "asset_type",
            "group",
            "position_value",
            "currency",
            "position_value_EUR",
            "position_dividend_yoy_EUR",
            "weight_pct",
        ],
        column_config={
            "ticker": "Ticker",
            "short_name": "Name",
            "asset_type": "Asset Type",
            "group": "Custom Group",
            "position_value": st.column_config.NumberColumn(
                "Value (Original Currency)",
                format="%.0f",
            ),
            "position_value_EUR": st.column_config.NumberColumn(
                "Value (EUR)",
                format="%.0f",
            ),
            "position_dividend_yoy_EUR": st.column_config.NumberColumn(
                "Dividends (Last 12M) €",
                format="%.0f",
            ),
            "weight_pct": st.column_config.NumberColumn(
                "Weight",
                format="%.1f %%",
            ),
            "currency": "Currency",
        },
        hide_index=True,
        use_container_width=True,
    )


def render_stock_composition_chart(df_history: pl.DataFrame) -> None:
    if df_history.is_empty():
        return
    df_latest = (
        df_history.sort("date")
        .group_by("ticker")
        .agg(
            pl.last("date"),
            pl.last("position_value_EUR"),
            # just dummies as they are same per ticker
            pl.last("asset_type"),
            pl.last("group"),
            pl.last("sector"),
            pl.last("country"),
            pl.last("short_name"),
        )
        .with_columns(
            pl.col("short_name")
            .fill_null(pl.col("ticker"))
            # no longer strings than 20 chars
            .str.slice(0, 25),
            pl.col("group").fill_null(pl.col("sector")).alias("color_category"),
        )
        .sort("position_value_EUR", descending=True)
    )
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Ticker Breakdown",
            "Sector Breakdown",
            "Country Breakdown",
            "Sector Simple",
            "Country Simple",
        ]
    )
    with tab1:
        fig_ticker = px.pie(
            df_latest,
            names="short_name",
            values="position_value_EUR",
            color="color_category",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )

        fig_ticker.update_layout(
            title="Portfolio Composition by Ticker",
            template="plotly_white",
            height=400,
        )

        st.plotly_chart(fig_ticker, use_container_width=True)
    with tab2:
        fig_sector = px.sunburst(
            df_latest,
            path=["sector", "short_name"],
            values="position_value_EUR",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        st.plotly_chart(fig_sector, use_container_width=True)
    with tab3:
        fig_country = px.sunburst(
            df_latest,
            path=["country", "short_name"],
            values="position_value_EUR",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        st.plotly_chart(fig_country, use_container_width=True)
    with tab4:
        fig_sector_simple = px.pie(
            df_latest,
            names="sector",
            values="position_value_EUR",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        st.plotly_chart(fig_sector_simple, use_container_width=True)
    with tab5:
        fig_country_simple = px.pie(
            df_latest,
            names="country",
            values="position_value_EUR",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        st.plotly_chart(fig_country_simple, use_container_width=True)


GLOBAL_MARGINS = dict(t=30, l=5, r=5, b=0)
GLOBAL_FONT = dict(family="Arial", size=16)


def make_sunburst_chart(df: pl.DataFrame, path: list[str], title: str | None = None) -> px.sunburst:
    fig = px.sunburst(
        df,
        path=path,
        values="position_value_EUR",
        color_discrete_sequence=COLOR_SCALE_CONTRAST,
        title=title,
    )
    fig.update_traces(
        insidetextorientation="horizontal",
        marker=dict(line=dict(color="#FFFFFF", width=2.0)),
    )
    fig.update_layout(
        height=400,
        margin=GLOBAL_MARGINS,
        showlegend=False,  # Stabilizes layout.
        font=GLOBAL_FONT,
        uniformtext=dict(
            minsize=10,  # If text < 10px is required to fit, hide it instead.
            mode="hide",  # options: 'hide' | 'show'
        ),
    )
    return fig


def make_pie_chart(df: pl.DataFrame, names: str, values: str, title: str | None = None) -> px.pie:
    fig = px.pie(
        df,
        names=names,
        values=values,
        color_discrete_sequence=COLOR_SCALE_CONTRAST,
        title=title,
    )
    fig.update_traces(
        textposition="inside",
        textinfo="label+percent",
        marker=dict(line=dict(color="#FFFFFF", width=2.0)),
    )
    fig.update_layout(
        height=400,
        margin=GLOBAL_MARGINS,
        showlegend=False,
        font=GLOBAL_FONT,
    )
    return fig


def render_portfolio_composition_chart(df_history: pl.DataFrame) -> None:
    """Render portfolio composition as pie chart.

    Args:
        df_history: Portfolio history with ticker-level positions
    """
    if df_history.is_empty():
        return

    # get complete row of the latest date per ticker
    df_latest = (
        df_history.sort("date")
        .group_by("ticker")
        .agg(
            pl.last("date"),
            pl.last("position_value_EUR"),
            # these are kind of dummies as they are same per ticker
            # but needed for the plots
            pl.last("asset_type"),
            pl.last("group"),
        )
        .with_columns(
            pl.col("asset_type").str.to_uppercase(),
        )
        .sort("position_value_EUR", descending=True)
    )

    has_group_usage = df_latest.select(pl.col("group").n_unique()).item() > 1
    n_col = 2 if has_group_usage else 1

    tab1, tab2, tab3 = st.tabs(["Asset Classes", "Groups", "Positions"])
    with tab1:
        cols = st.columns(n_col)
        with cols[0]:
            fig_asset_simple = make_pie_chart(
                df_latest,
                names="asset_type",
                values="position_value_EUR",
            )
            st.plotly_chart(fig_asset_simple, use_container_width=True)
        if has_group_usage:
            with cols[1]:
                fig_asset = make_sunburst_chart(
                    df_latest,
                    path=["asset_type", "group"],
                )
                st.plotly_chart(fig_asset, use_container_width=True)
    with tab2:
        cols = st.columns(n_col)
        with cols[0]:
            fig_group_simple = make_pie_chart(
                df_latest,
                names="group",
                values="position_value_EUR",
            )
            st.plotly_chart(fig_group_simple, use_container_width=True)
        if has_group_usage:
            with cols[1]:
                fig_group = make_sunburst_chart(
                    df_latest,
                    path=["group", "asset_type"],
                )
                st.plotly_chart(fig_group, use_container_width=True)

    with tab3:
        fig_pos = make_pie_chart(
            df_latest,
            names="ticker",
            values="position_value_EUR",
        )

        st.plotly_chart(fig_pos, use_container_width=True)


def render_market_snapshot_table(
    df_snapshot: pl.DataFrame,
) -> None:
    """Render market fundamentals table.

    Args:
        df_snapshot: DataFrame with market fundamentals data
    """
    if df_snapshot.is_empty():
        st.warning("No market fundamentals data to display")
        return
    df_snapshot = df_snapshot.filter(pl.col("asset_type") == AssetType.STOCK).with_columns(
        (
            pl.col("country").replace(COUNTRY_FLAGS, default="❓")
            + " "
            + pl.col("sector").replace(SECTOR_EMOJI, default="👻")
        ).alias("info")
    )

    st.dataframe(
        df_snapshot,
        column_order=[
            "short_name",
            "ticker",
            "info",
            "market_cap_b_eur",
            "pe_ratio",
            "fcf_yield",
            "roce",
            "gross_margin",
            "ebit_margin",
            "revenue_growth",
            "net_debt_to_ebit",
        ],
        column_config={
            "market_cap_b_eur": st.column_config.NumberColumn("Market Cap 💶", format="%.1f B€"),
            "info": "",
            "ticker": "Ticker",
            "short_name": "Name",
            "pe_ratio": st.column_config.NumberColumn("P/E 💰", format="%.1f"),
            "fcf_yield": st.column_config.NumberColumn("FCF Yield 💰", format="%.2f%%"),
            "gross_margin": st.column_config.NumberColumn("Gross Margin 💎", format="%.1f%%"),
            "ebit_margin": st.column_config.NumberColumn("EBIT Margin 💎", format="%.1f%%"),
            "roce": st.column_config.NumberColumn("ROCE 💎", format="%.1f%%"),
            "revenue_growth": st.column_config.NumberColumn("Revenue Growth 🚀", format="%.1f%%"),
            "net_debt_to_ebit": st.column_config.NumberColumn("Debt/EBIT 🏥", format="%.1fx"),
        },
    )
