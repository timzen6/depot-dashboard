import pandas as pd
import polars as pl
import streamlit as st

from src.app.views.colors import COLOR_SCALE_GREEN_RED, Colors
from src.app.views.constants import assign_info_emojis
from src.config.landing_page import LandingPageConfig


def render_portfolio_overview_table(
    df_portfolio: pl.DataFrame,
) -> None:
    st.dataframe(
        df_portfolio,
        column_config={
            "portfolio_name": "Name",
            "current": st.column_config.NumberColumn("Current Value", format="%.0f"),
            "current_yoy_dividend": st.column_config.NumberColumn("YoY Dividend", format="%.0f"),
            "yoy_return": st.column_config.NumberColumn("YoY Return", format="%.1f%%"),
            "usa_percentage": st.column_config.NumberColumn("🇺🇸 %", format="%.1f%%", width="small"),
            "europe_percentage": st.column_config.NumberColumn(
                "🇪🇺 %", format="%.1f%%", width="small"
            ),
            "stock_percentage": st.column_config.NumberColumn("Stocks %", format="%.1f%%"),
            "tech": st.column_config.NumberColumn("🔬 %", format="%.0f%%", width="small"),
            "stab": st.column_config.NumberColumn("🛡️ %", format="%.0f%%", width="small"),
            "real": st.column_config.NumberColumn("⚙️ %", format="%.0f%%", width="small"),
            "price": st.column_config.NumberColumn("👜 %", format="%.0f%%", width="small"),
        },
    )


def color_pe_rank(val: float) -> str:
    if pd.isna(val):
        return ""
    color = COLOR_SCALE_GREEN_RED[3]
    val = val * 100  # convert to percentage
    if val < 35:
        color = COLOR_SCALE_GREEN_RED[1]
    elif val < 65:
        color = Colors.white

    return f"background-color: {color}; color: black"


def color_data_lag(val: float) -> str:
    if pd.isna(val):
        return ""
    if val <= 180:
        color = Colors.white
    else:
        color = COLOR_SCALE_GREEN_RED[3]
    return f"background-color: {color}; color: black"


def render_stocks_to_watch_table(
    df_snapshot: pl.DataFrame,
) -> None:
    df_snapshot_pandas = (
        df_snapshot.pipe(assign_info_emojis, "sector", "country", "asset_type", "name")
        .with_columns(pl.col("upside") * 100)
        .to_pandas()
    )

    styler = df_snapshot_pandas.style.apply(
        lambda _: df_snapshot_pandas["pe_rank"].apply(color_pe_rank),
        subset=["pe_ratio"],
    ).apply(
        lambda _: df_snapshot_pandas["data_lag_days"].apply(color_data_lag),
        subset=["data_lag_days"],
    )

    st.dataframe(
        styler,
        hide_index=True,
        column_order=[
            "ticker_emoji",
            "ticker",
            "name",
            "info",
            "close",
            "fair_value",
            "upside",
            "pe_ratio",
            "data_lag_days",
            # "close_30d",
        ],
        height="content",
        column_config={
            "ticker_emoji": st.column_config.TextColumn("", width="small"),
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "name": st.column_config.TextColumn("Name", width="medium"),
            "info": st.column_config.TextColumn("Info", width="small"),
            "close": st.column_config.NumberColumn("Price €", format="%.1f"),
            "fair_value": st.column_config.NumberColumn("Fair Value €", format="%.0f"),
            "upside": st.column_config.ProgressColumn(
                "💰 Upside",
                min_value=-50,
                max_value=50,
                format="%.0f%%",
                color="auto",
                width="small",
            ),
            "close_30d": st.column_config.LineChartColumn(
                "📈 30d Price Chart", width="medium", color="auto"
            ),
            "pe_ratio": st.column_config.NumberColumn("P/E Ratio", format="%.1f"),
            "data_lag_days": st.column_config.NumberColumn(
                "Data Lag", format="%.0f", width="small"
            ),
        },
    )


def render_watch_list_alert_tables(df_watch: pl.DataFrame) -> None:
    column_config = {
        "ticker": st.column_config.TextColumn("Ticker", width="small"),
        "close_EUR": st.column_config.NumberColumn("Price €", format="%.1f"),
        "pe_ratio": st.column_config.NumberColumn("P/E Ratio", format="%.1f"),
        "upside": st.column_config.ProgressColumn(
            "💰 Upside",
            min_value=-50,
            max_value=50,
            format="%.0f%%",
            color="auto",
            width="small",
        ),
        "alert": st.column_config.TextColumn("Alert", width="medium"),
    }
    tab1, tab2 = st.tabs(["Alerts", "Set Alerts"])
    with tab1:
        display_order = [
            "ticker",
            "close_EUR",
            "pe_ratio",
            "upside",
            "alert",
        ]
        st.markdown(""" ### Buy or Increase Positions""")
        st.dataframe(
            df_watch.filter((pl.col("action") == "buy") & (pl.col("alert").is_not_null())),
            column_order=display_order,
            column_config=column_config,
        )

        st.markdown(""" ### Sell or Decrease Positions""")

        st.dataframe(
            df_watch.filter((pl.col("action") == "sell") & (pl.col("alert").is_not_null())),
            column_order=display_order,
            column_config=column_config,
        )
    with tab2:
        st.markdown("### 🔍 Stocks to Watch")
        df_watch_pandas = df_watch.to_pandas()
        styler = df_watch_pandas.style.apply(
            lambda _: df_watch_pandas["alert"].apply(
                lambda val: (
                    f"background-color: {COLOR_SCALE_GREEN_RED[0]}; color: white"
                    if pd.notna(val)
                    else ""
                )
            ),
            subset=["alert", "action", "metric", "threshold", "ticker"],
        )
        st.dataframe(
            styler,
            height="content",
            column_order=[
                "ticker",
                "action",
                "metric",
                "threshold",
                "alert",
            ],
            column_config=column_config,
        )


def render_info_section(landing_config: LandingPageConfig) -> None:
    col1, col2 = st.columns([3, 2])
    with col1:
        st.subheader("📜 The Quality Core Strategy")
        if landing_config.strategy is not None:
            etf_strategy = landing_config.strategy.foundation
            st.markdown(f"### {etf_strategy.name}")
            st.markdown(etf_strategy.description)
            for component in etf_strategy.components:
                st.markdown(f"**{component.name}:** {component.detail}")

            stock_strategy = landing_config.strategy.quality_core
            st.markdown(f"### {stock_strategy.name}")
            st.markdown(stock_strategy.description)
            for pillar in stock_strategy.pillars:
                st.markdown(f"**{pillar.name}:** {pillar.detail}")
        execution_rules = landing_config.execution
        if execution_rules is not None:
            st.markdown("### 📐 The Execution Rules")
            for rule in execution_rules:
                st.markdown(f"**{rule.title}:** {rule.text}")

    with col2:
        st.subheader("📊 The Quality Factors")
        for _, factor in landing_config.factors.items():
            st.markdown(f"### {factor.icon} {factor.title}")
            st.markdown(f"**Description:** {factor.description}")
            st.markdown(f"**Test Question:** {factor.test_question}")
            st.markdown(f"**Indicators:** {factor.indicators}")
            st.markdown(f"**Examples:** {factor.examples}")
