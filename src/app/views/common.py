"""Common UI components shared across pages.

Pure rendering functions for reusable Streamlit widgets.
"""

import streamlit as st

from src.app.logic.overview import PortfolioKPIs
from src.config.models import Portfolio


def portfolio_selection(
    portfolios: list[Portfolio],
    on_sidebar: bool = True,
    allow_none: bool = False,
    default_index: int = 1,
) -> Portfolio | None:
    """Render a portfolio selection dropdown in the sidebar.

    Args:
        portfolios: List of Portfolio objects to choose from

    Returns:
        Selected Portfolio object or None if no selection
    """
    portfolio_options: dict[str, Portfolio | None] = {p.ui_name: p for p in portfolios}
    if allow_none:
        portfolio_options = {"-- None --": None, **portfolio_options}
    if on_sidebar:
        selected_ui_name = st.sidebar.selectbox(
            "Select Portfolio",
            options=list(portfolio_options.keys()),
            index=default_index,
        )
    else:
        selected_ui_name = st.selectbox(
            "Select Portfolio",
            options=list(portfolio_options.keys()),
            index=default_index,
        )
    return portfolio_options.get(selected_ui_name)


def render_kpi_cards(metrics: PortfolioKPIs) -> None:
    """Render key performance indicators as metric cards.

    Displays metrics in a responsive column layout using Streamlit's
    native metric component for consistent styling.

    Args:
        metrics: Dictionary with keys:
            - current_value: Current portfolio/ticker value
            - start_value: Starting value
            - total_return_pct: Total return percentage
            - yoy_return_pct: Year-over-year return percentage
            - latest_date: Most recent data date
    """
    cols = st.columns(6)

    with cols[0]:
        st.metric(
            label="Current Value",
            value=f"{metrics.current_value:,.0f} €",
        )

    with cols[1]:
        st.metric(
            label="Total Return",
            value=f"{metrics.total_return_pct:+.1f}%",
            delta=f"{metrics.total_return_pct:+.1f}%",
        )

    with cols[2]:
        st.metric(
            label="YoY Return",
            value=f"{metrics.yoy_return_pct:+.2f}%",
            delta=f"{metrics.yoy_return_pct:+.2f}%",
        )

    with cols[3]:
        st.metric(
            label="Latest Update",
            value=str(metrics.latest_date),
        )
    with cols[4]:
        st.metric(
            label="Start Date",
            value=str(metrics.start_date),
        )

    yoy_dividend = metrics.current_yoy_dividend_value
    start_value = metrics.start_value
    if start_value == 0:
        dividend_percent = 0.0
    else:
        dividend_percent = (yoy_dividend / start_value * 100) if start_value != 0 else 0.0
    with cols[5]:
        st.metric(
            label="Dividends (Last 12M)",
            value=(f"{metrics.current_yoy_dividend_value:,.0f} €" f" ({dividend_percent:.2f}%)"),
        )


def render_sidebar_header(title: str, description: str | None = None) -> None:
    """Render consistent sidebar header with optional description.

    Args:
        title: Main sidebar title
        description: Optional description text below title
    """
    st.sidebar.title(title)
    if description:
        st.sidebar.caption(description)
    st.sidebar.divider()


def render_empty_state(message: str, icon: str = "📊") -> None:
    """Render empty state placeholder when no data is available.

    Args:
        message: Message to display
        icon: Emoji icon to show
    """
    st.info(f"{icon} {message}")
