"""Common UI components shared across pages.

Pure rendering functions for reusable Streamlit widgets.
"""

import streamlit as st


def render_kpi_cards(metrics: dict[str, float | str]) -> None:
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
    cols = st.columns([1, 1, 1, 1, 1])

    with cols[0]:
        st.metric(
            label="Current Value",
            value=f"{metrics.get('current_value', 0):,.0f} €",
        )

    with cols[1]:
        st.metric(
            label="Total Return",
            value=f"{metrics.get('total_return_pct', 0):+.1f}%",
            delta=f"{metrics.get('total_return_pct', 0):+.1f}%",
        )

    with cols[2]:
        st.metric(
            label="YoY Return",
            value=f"{metrics.get('yoy_return_pct', 0):+.2f}%",
            delta=f"{metrics.get('yoy_return_pct', 0):+.2f}%",
        )

    with cols[3]:
        st.metric(
            label="Latest Update",
            value=str(metrics.get("latest_date", "N/A")),
        )
    with cols[4]:
        st.metric(
            label="Start Date",
            value=str(metrics.get("start_date", "N/A")),
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
