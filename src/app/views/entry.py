from datetime import date

import plotly.graph_objects as go
import polars as pl
import streamlit as st
from dateutil.relativedelta import relativedelta
from plotly.subplots import make_subplots

from src.app.logic.data_loader import DashboardData
from src.app.views.colors import Colors
from src.app.views.constants import assign_info_emojis
from src.app.views.screener import key_to_selected_tickers
from src.core.domain_models import AssetType


def highlight_valuation_ranks(val: float) -> str:
    if val is None:
        return ""
    if val <= 0.20:
        return f"background-color: {Colors.light_green}; color: black; font-weight: bold"
    if val >= 0.80:
        return f"background-color: {Colors.light_red}; color: black"
    return ""


def classify_volatility(v: float | None) -> str:
    """
    Classifies annualized volatility based on Quality-Investing standards.
    """
    if v is None:
        return "N/A"

    # < 18%: Very stable (e.g., Consumer Staples, Pharma)
    if v < 18.0:
        return "🛡️ Low (Stable)"

    # 18% - 25%: Market average (e.g., Microsoft, Siemens)
    if v < 25.0:
        return "⚖️ Medium"

    # 25% - 40%: Increased risk (e.g., Cyclicals, High Growth)
    if v < 40.0:
        return "🌊 High (Volatile)"

    # > 40%: Highly speculative (e.g., Biotech, Crypto, Turnaround)
    return "⚠️ Extreme (Speculative)"


def classify_percentile(p: float | None) -> str:
    if p is None:
        return "N/A (Data < 1y)"
    if p < 0.10:
        return "💎 Bargain (<10%)"
    if p < 0.30:
        return "✅ Attractive"
    if p < 0.70:
        return "➡️ Fair Value"
    if p < 0.90:
        return "↗️ Expensive"
    return "🔥 Overextended (>90%)"


def classify_z_score(z: float) -> str:
    if z is None:
        return "N/A"
    if z <= -2.0:
        return "📉 Oversold (Extreme)"
    if -2.0 < z <= -1.0:
        return "↘️ Undervalued (Slight)"
    if -1.0 < z < 1.0:
        return "➡️ Normal Range"
    if 1.0 <= z < 2.0:
        return "↗️ Overvalued (Slight)"
    if z >= 2.0:
        return "📈 Overbought (Extreme)"
    return "N/A"


def render_sidebar_selection(
    dashboard_data: DashboardData, portfolio_dict: dict[str, list[str]]
) -> tuple[date, int, list[str] | None]:
    """Render sidebar selection controls for the entry analysis page."""
    with st.sidebar:
        st.header("⚙️ Settings")

        # 1. Time Filter
        time_range = st.radio(
            "Observation Period",
            options=["6 Months", "12 Months", "18 Months", "3 Years", "All Time"],
            index=1,
        )

        # Calculate cutoff date
        max_date = dashboard_data.prices.select(pl.col("date").max()).item()
        cutoff_date: date = date(2000, 1, 1)
        if time_range == "6 Months":
            cutoff_date = max_date - relativedelta(months=6)
        elif time_range == "12 Months":
            cutoff_date = max_date - relativedelta(months=12)
        elif time_range == "18 Months":
            cutoff_date = max_date - relativedelta(months=18)
        elif time_range == "3 Years":
            cutoff_date = max_date - relativedelta(years=3)

        st.divider()

        # 2. Limit Calculator Settings
        st.subheader("🎯 Limit Logic")
        windows = {
            "1 Day": 1,
            "1 Week (5 Days)": 5,
            "1 Month (20 Days)": 20,
        }
        selected_window_label = st.radio(
            "Execution Window",
            list(windows.keys()),
            horizontal=False,
            index=1,
        )
        window_days = windows[selected_window_label]

        st.divider()
        filter_portfolios = st.multiselect(
            "Filter by Portfolios (optional)",
            options=(list(portfolio_dict.keys())),
            default=[],
        )
        if filter_portfolios:
            selected_tickers_set = set()
            for pf in filter_portfolios:
                selected_tickers_set.update(portfolio_dict.get(pf, []))
            selected_tickers = list(selected_tickers_set)
        else:
            selected_tickers = None

        return cutoff_date, window_days, selected_tickers


def render_stock_selection(
    dashboard_data: DashboardData,
    selected_tickers: list[str] | None,
) -> list[str]:
    """Render stock selection control and return filtered metadata."""
    filtered_stock_metadata = dashboard_data.metadata.filter(
        pl.col("asset_type") == AssetType.STOCK
    )
    if selected_tickers is not None:
        filtered_stock_metadata = filtered_stock_metadata.filter(
            pl.col("ticker").is_in(selected_tickers)
        )
    stock_metadata = (
        filtered_stock_metadata.pipe(
            assign_info_emojis, "sector", "country", "asset_type", "name"
        ).with_columns(
            pl.coalesce(
                pl.col("display_name"),
                pl.col("name"),
                pl.col("short_name"),
                pl.col("ticker"),
            ).alias("name")
        )
    ).select(["ticker", "name", "info", "country", "forward_pe"])

    st.subheader("1️⃣ Select Tickers")
    st.dataframe(
        stock_metadata,
        selection_mode="multi-row",
        key="entry_analysis_selection",
        on_select="rerun",
        use_container_width=True,
        height=300,
    )

    selected_tickers = key_to_selected_tickers(
        "entry_analysis_selection",
        stock_metadata,
        return_all_if_none=False,
    )
    return selected_tickers


def render_trend_analysis_charts(
    selected_price_data: pl.DataFrame,
    selected_tickers: list[str],
    ticker_corridors: dict[str, tuple[float | None, float | None]],
) -> None:
    """Render trend analysis charts for selected tickers."""
    n_tickers = len(selected_tickers)
    fig = make_subplots(
        rows=n_tickers * 2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08 / n_tickers if n_tickers > 1 else 0.1,
        row_heights=[0.7, 0.3] * n_tickers,
        subplot_titles=[
            f"{t} Price & SMA200" if i % 2 == 0 else f"{t} Valuation Corridor (3y)"
            for t in selected_tickers
            for i in range(2)
        ],
    )

    for i, ticker in enumerate(selected_tickers):
        df_t = selected_price_data.filter(pl.col("ticker") == ticker)

        # Plotly indices
        row_p = (i * 2) + 1
        row_v = (i * 2) + 2

        # A) PRICE CHART
        # SMA 200 (The "Fair Value" path)
        fig.add_trace(
            go.Scatter(
                x=df_t["date"],
                y=df_t["sma_200"],
                line=dict(color=Colors.gray, width=1, dash="dot"),
                name="SMA 200",
                showlegend=(i == 0),
            ),
            row=row_p,
            col=1,
        )
        # SMA 50
        fig.add_trace(
            go.Scatter(
                x=df_t["date"],
                y=df_t["sma_50"],
                line=dict(color=Colors.amber, width=1),
                name="SMA 50",
                showlegend=(i == 0),
            ),
            row=row_p,
            col=1,
        )
        # Close
        fig.add_trace(
            go.Scatter(
                x=df_t["date"],
                y=df_t["close"],
                line=dict(color=Colors.blue, width=2),
                name="Close",
                showlegend=(i == 0),
            ),
            row=row_p,
            col=1,
        )

        # B) VALUATION RANK CHART (Abstand zum SMA200)
        fig.add_trace(
            go.Bar(
                x=df_t["date"],
                y=df_t["dist_200_pct"] * 100,
                name="Dist to SMA200 %",
                marker_color=Colors.purple,
                showlegend=(i == 0),
            ),
            row=row_v,
            col=1,
        )
        # Zero line (touching SMA200)
        fig.add_hline(y=0, line_color="black", row=row_v, col=1)

        # --- VISUAL CORRIDOR (P10 / P90) ---
        p10, p90 = ticker_corridors.get(ticker, (None, None))
        if p10 is not None and p90 is not None:
            # P10 Line (Green - Cheap Zone)
            fig.add_hline(
                y=p10 * 100,
                line_dash="dash",
                line_color="green",
                line_width=1,
                annotation_text="P10 (Cheap)",
                annotation_position="bottom right",
                row=row_v,
                col=1,
            )
            # P90 Line (Red - Expensive Zone)
            fig.add_hline(
                y=p90 * 100,
                line_dash="dash",
                line_color="red",
                line_width=1,
                annotation_text="P90 (Exp.)",
                annotation_position="top right",
                row=row_v,
                col=1,
            )

    fig.update_layout(height=450 * n_tickers, margin=dict(t=30, b=20))
    st.plotly_chart(fig, use_container_width=True)


def render_tactic_strategic_overview_table(
    df_status: pl.DataFrame,
) -> None:
    """Render tactical & strategic overview table."""
    df_status = df_status.with_columns(
        pl.col("valuation_rank")
        .map_elements(classify_percentile)
        .alias("valuation_classification"),
        pl.col("z_score").map_elements(classify_z_score).alias("tactical_classification"),
        pl.col("vola_annual_pct")
        .map_elements(classify_volatility)
        .alias("volatility_classification"),
    )
    st.dataframe(
        df_status,
        use_container_width=True,
        hide_index=True,
        column_order=[
            "ticker",
            "price",
            "trend_dist",
            "valuation_rank",
            "valuation_classification",
            "z_score",
            "tactical_classification",
            "vola_annual_pct",
            "volatility_classification",
        ],
        column_config={
            "ticker": st.column_config.TextColumn("Ticker"),
            "price": st.column_config.NumberColumn("Price", format="%.2f"),
            # 1. TACTICAL (Kurzfristig)
            "z_score": st.column_config.NumberColumn(
                "Tactical Z-Score(50d)",
                format="%.1f",
                help=(
                    "Short-term timing indicator based on 50-day price deviations."
                    ">2 = running hot, < -2 = panic.",
                ),
            ),
            # 2. STRATEGIC (Langfristig / Valuation)
            "trend_dist": st.column_config.NumberColumn(
                "Dist SMA200",
                format="%.1f%%",
            ),
            "valuation_rank": st.column_config.ProgressColumn(
                "Hist. Valuation (3y)",
                format="%.2f",
                min_value=0,
                max_value=1,
                help=(
                    "Percentile Rank (3y). 0.10 = The stock was historically only"
                    " 10% of the time this cheap (relative to the trend).",
                ),
            ),
            "valuation_classification": st.column_config.TextColumn(
                "Valuation Class",
            ),
            # 3. RISK (Charakter)
            "vola_annual_pct": st.column_config.NumberColumn(
                "Vola p.a.",
                format="%.1f%%",
                help="< 18% = Sehr stabil (Core). > 25% = Volatil.",
            ),
            "tactical_classification": st.column_config.TextColumn(
                "Tactical Class",
            ),
            "volatility_classification": st.column_config.TextColumn(
                "Volatility Class",
            ),
        },
    )
    st.caption(
        """
        **Interpretation Guide:**
        * **Hist. Valuation (Rank):** Ignores the absolute price and checks:
        *"Is the distance to the trend historically cheap?"*
            * **Low bar (< 0.2):** Historical buying opportunity (mean reversion).
            * **N/A:** Not enough data (< 100 days) for statistical significance.
        * **Tactical Z:** Timing signal. For quality stocks, often 0.0 to -0.5
        is already a good entry (pullback).
        """
    )
