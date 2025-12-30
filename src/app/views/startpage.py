from datetime import date, timedelta

import pandas as pd
import polars as pl
import streamlit as st

from src.app.logic.data_loader import DashboardData
from src.app.views.colors import COLOR_SCALE_GREEN_RED, Colors
from src.app.views.constants import assign_info_emojis
from src.config.landing_page import LandingPageConfig
from src.core.domain_models import ReportType


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


def render_recent_reports_section(data: DashboardData, selected_ticker: list[str]) -> None:
    tmp_meta = (
        data.metadata.filter(pl.col("ticker").is_in(selected_ticker))
        .select(["ticker", "display_name", "short_name", "earnings_date", "dividend_date"])
        .with_columns(pl.coalesce(pl.col("display_name"), pl.col("short_name")).alias("name"))
        .drop("short_name", "display_name")
    )
    tmp_fund = (
        (
            data.fundamentals.filter(pl.col("ticker").is_in(selected_ticker))
            .select(["ticker", "date", "period_type"])
            .sort(["ticker", "date"], descending=False)
        )
        .filter(pl.col("period_type") == ReportType.ANNUAL)
        .group_by("ticker")
        .agg(pl.col("date").last())
        .with_columns(
            # eastimated next fiscal year end by adding 1 year
            (pl.col("date") + pl.duration(days=365)).alias("est_next_annual_earning")
        )
        .rename({"date": "last_annual_earning"})
    )

    tmp = tmp_meta.join(tmp_fund, on="ticker", how="left").sort(
        ["est_next_annual_earning", "ticker"]
    )
    # for now we take the estimated next annual earning to
    # have annual report alerts consistently
    # (quarterly reports are not always available and less relevant in general)
    today = date.today()
    end_lookup = today + timedelta(days=30)
    tmp_next_earnings = tmp.filter(
        (pl.col("est_next_annual_earning") >= today)
        & (pl.col("est_next_annual_earning") <= end_lookup)
    )
    if tmp_next_earnings.is_empty():
        st.info("No upcoming earnings dates in the next 30 days.")
    else:
        st.subheader("Upcoming Earnings Dates")
        st.dataframe(
            tmp_next_earnings,
            column_order=["ticker", "name", "est_next_annual_earning"],
            column_config={
                "est_next_annual_earning": st.column_config.DateColumn(
                    "Next Annual Earnings",
                    format="YYYY-MM-DD",
                ),
                "ticker": "Ticker",
                "name": "Company Name",
            },
        )
    tmp_recent_earnings = tmp.filter(
        pl.col("last_annual_earning") >= today - timedelta(days=60)
    ).sort(["last_annual_earning", "ticker"], descending=True)
    if tmp_recent_earnings.is_empty():
        st.info("No recent earnings reports.")
    else:
        st.subheader("Recently Reported Earnings")
        st.dataframe(
            tmp_recent_earnings,
            column_order=["ticker", "name", "last_annual_earning"],
            column_config={
                "last_annual_earning": st.column_config.DateColumn(
                    "Last Annual Earnings",
                    format="YYYY-MM-DD",
                ),
                "ticker": "Ticker",
                "name": "Company Name",
            },
        )


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
            "forward_pe",
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
            "forward_pe": st.column_config.NumberColumn("Fwd P/E", format="%.1f"),
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
        st.subheader("Buy or Increase Positions")
        df_watch_buy_pandas = df_watch.filter(
            (pl.col("action") == "buy") & (pl.col("alert").is_not_null())
        ).to_pandas()
        styler = df_watch_buy_pandas.style.apply(
            lambda _: df_watch_buy_pandas["alert"].apply(
                lambda val: (
                    f"background-color: {COLOR_SCALE_GREEN_RED[1]}; color: black"
                    if val.startswith("GOOD")
                    else ""
                )
            ),
            subset=["alert"],
        )

        st.dataframe(
            styler,
            column_order=display_order,
            column_config=column_config,
        )

        st.subheader("Sell or Decrease Positions")
        df_watch_sell_pandas = df_watch.filter(
            (pl.col("action") == "sell") & (pl.col("alert").is_not_null())
        ).to_pandas()
        styler = df_watch_sell_pandas.style.apply(
            lambda _: df_watch_sell_pandas["alert"].apply(
                lambda val: (
                    f"background-color: {COLOR_SCALE_GREEN_RED[3]}; color: black"
                    if val.startswith("GOOD")
                    else ""
                )
            ),
            subset=["alert"],
        )

        st.dataframe(
            styler,
            column_order=display_order,
            column_config=column_config,
        )
    with tab2:
        st.markdown("### 🔍 Stocks to Watch")
        df_watch_pandas = df_watch.to_pandas()
        styler = df_watch_pandas.style.apply(
            lambda _: df_watch_pandas["alert"].apply(
                lambda val: (
                    ""
                    if pd.isna(val)
                    else (
                        f"background-color: {COLOR_SCALE_GREEN_RED[1]}; color: black"
                        if val.startswith("GOOD")
                        else (
                            f"background-color: {COLOR_SCALE_GREEN_RED[2]}; color: black"
                            if val.startswith("FAIR")
                            else ""
                        )
                    )
                )
            ),
            subset=[
                "alert",
                "action",
                "metric",
                "fair_threshold",
                "good_threshold",
                "ticker",
            ],
        )
        st.dataframe(
            styler,
            height="content",
            column_order=[
                "ticker",
                "action",
                "metric",
                "fair_threshold",
                "good_threshold",
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
