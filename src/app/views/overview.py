"""View components for portfolio overview page.

Renders portfolio performance charts and position tables.
"""

import plotly.express as px
import polars as pl
import streamlit as st
from views.colors import Colors

from src.app.logic.overview import filter_days_with_incomplete_tickers


def render_portfolio_chart(df_history: pl.DataFrame) -> None:
    """Render portfolio value over time as interactive line chart.

    Args:
        df_history: Portfolio history with columns [date, total_value]
    """
    if df_history.is_empty():
        st.warning("No portfolio history data to display")
        return

    df_plot = (
        df_history.pipe(filter_days_with_incomplete_tickers)
        .group_by("date")
        .agg(pl.sum("position_value").alias("total_value"))
        .sort("date")
    )

    # Create line chart
    fig = px.line(
        df_plot,
        x="date",
        y="total_value",
        title="Portfolio Value Over Time",
        labels={"total_value": "Portfolio Value ($)", "date": "Date"},
    )

    fig.update_layout(
        hovermode="x unified",
        template="plotly_white",
        height=500,
    )

    fig.update_traces(
        line=dict(width=2, color="#1f77b4"),
        fill="tozeroy",
        fillcolor="rgba(31, 119, 180, 0.1)",
    )

    st.plotly_chart(fig, use_container_width=True)


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
        ["ticker", "position_value", "currency"]
    )
    df_current = (
        df_history.sort("date")
        .group_by("ticker")
        .agg(
            pl.last("position_value").alias("position_value"),
            pl.last("currency").alias("currency"),
            pl.last("position_value_EUR").alias("position_value_EUR"),
        )
    )

    # Calculate weights
    total_value = df_current.select(pl.sum("position_value_EUR")).item()

    df_display = df_current.with_columns(
        (pl.col("position_value_EUR") / total_value * 100).alias("weight_pct")
    ).sort("position_value_EUR", descending=True)

    st.dataframe(
        df_display,
        column_config={
            "ticker": "Ticker",
            "position_value": "Value (Original Currency)",
            "position_value_EUR": "Value (EUR)",
            "weight_pct": "Weight",
            "currency": "Currency",
        },
        hide_index=True,
        use_container_width=True,
    )


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
        )
        .sort("position_value_EUR", descending=True)
    )

    fig = px.pie(
        df_latest,
        names="ticker",
        values="position_value_EUR",
        color_discrete_sequence=[
            Colors.blue,
            Colors.orange,
            Colors.green,
            Colors.red,
            Colors.purple,
            Colors.yellow,
            Colors.light_blue,
            Colors.amber,
        ],
    )

    fig.update_layout(
        title="Portfolio Composition",
        template="plotly_white",
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)
