"""View components for portfolio overview page.

Renders portfolio performance charts and position tables.
"""

import plotly.express as px
import polars as pl
import streamlit as st
from views.colors import COLOR_SCALE_CONTRAST, STRATEGY_FACTOR_COLOR_MAP, Colors

from src.app.logic.overview import filter_days_with_incomplete_tickers
from src.app.views.common import (
    GLOBAL_FONT,
    GLOBAL_MARGINS,
    make_pie_chart,
    make_sunburst_chart,
    style_pie_chart,
)
from src.app.views.constants import COUNTRY_FLAGS, SECTOR_EMOJI
from src.core.domain_models import AssetType
from src.core.strategy_engine import StrategyEngine


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
            color_discrete_map={
                "Defensive": COLOR_SCALE_CONTRAST[0],
                "Tech": COLOR_SCALE_CONTRAST[1],
                "Industrial": COLOR_SCALE_CONTRAST[2],
                "Finance": COLOR_SCALE_CONTRAST[3],
                "Luxury": COLOR_SCALE_CONTRAST[4],
                "ETF": COLOR_SCALE_CONTRAST[0],
                "STOCK": COLOR_SCALE_CONTRAST[1],
            },
        )
        fig.update_layout(legend_title_text="")
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


def render_positions_table(df_latest: pl.DataFrame, portfolio_name: str) -> None:
    """Render current portfolio positions as an interactive table.

    Shows latest position values and weights for each ticker.

    Args:
        df_latest: Latest portfolio positions with ticker-level data
        portfolio_name: Portfolio identifier for display
    """
    if df_latest.is_empty():
        st.warning("No position data to display")
        return

    st.subheader(f"💼 Current Positions - {portfolio_name}")

    # Calculate weights
    total_value = df_latest.select(pl.sum("position_value_EUR")).item()

    df_display = df_latest.with_columns(
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


def render_stock_composition_chart(
    df_latest: pl.DataFrame, strategy_engine: StrategyEngine
) -> None:
    if df_latest.is_empty():
        return
    df_latest = df_latest.with_columns(
        pl.col("short_name")
        .fill_null(pl.col("ticker"))
        # no longer strings than 20 chars
        .str.slice(0, 25),
        pl.col("group").fill_null(pl.col("sector")).alias("color_category"),
    ).sort("position_value_EUR", descending=True)

    tab_names = [
        "Strategy Factors",
        "Ticker Breakdown",
        "Sector Breakdown",
        "Country Breakdown",
        "Sector Simple",
        "Country Simple",
    ]

    tabs = st.tabs(tab_names)
    with tabs[1]:
        fig_ticker = px.pie(
            df_latest,
            names="short_name",
            values="position_value_EUR",
            color="color_category",
            color_discrete_map={
                "Defensive": COLOR_SCALE_CONTRAST[0],
                "Tech": COLOR_SCALE_CONTRAST[1],
                "Industrial": COLOR_SCALE_CONTRAST[2],
                "Finance": COLOR_SCALE_CONTRAST[3],
                "Luxury": COLOR_SCALE_CONTRAST[4],
            },
        )

        fig_ticker.update_layout(
            title="Portfolio Composition by Ticker",
            template="plotly_white",
            height=400,
            margin=GLOBAL_MARGINS,
            font=GLOBAL_FONT,
        )
        fig_ticker.update_traces(
            textposition="inside",
            textinfo="percent",
            marker=dict(line=dict(color="#FFFFFF", width=2.0)),
        )

        st.plotly_chart(fig_ticker, use_container_width=True)
    with tabs[2]:
        fig_sector = make_sunburst_chart(
            df_latest,
            path=["sector", "short_name"],
            title="Portfolio Composition by Sector",
        )
        st.plotly_chart(fig_sector, use_container_width=True)
    with tabs[3]:
        fig_country = make_sunburst_chart(
            df_latest,
            path=["country", "short_name"],
            title="Portfolio Composition by Country",
        )
        st.plotly_chart(fig_country, use_container_width=True)
    with tabs[4]:
        fig_sector_simple = make_pie_chart(
            df_latest,
            names="sector",
            values="position_value_EUR",
        )
        st.plotly_chart(fig_sector_simple, use_container_width=True)
    with tabs[5]:
        fig_country_simple = make_pie_chart(
            df_latest,
            names="country",
            values="position_value_EUR",
        )
        st.plotly_chart(fig_country_simple, use_container_width=True)
    with tabs[0]:
        df_factors = (
            strategy_engine.calculate_portfolio_exposure(
                df_latest, value_column="position_value_EUR", sector_column="sector"
            )
            .filter(pl.col("value") > 0.0)
            .with_columns(pl.col("factor").str.split(" / ").list.first().alias("factor"))
        )
        fig_strategy = px.pie(
            df_factors,
            names="factor",
            values="value",
            color="factor",
            color_discrete_map=STRATEGY_FACTOR_COLOR_MAP,
        )
        style_pie_chart(fig_strategy)
        st.plotly_chart(fig_strategy, use_container_width=True)


def render_portfolio_composition_chart(
    df_latest: pl.DataFrame,
    df_etf_sectors: pl.DataFrame,
    df_etf_countries: pl.DataFrame,
    strategy_engine: StrategyEngine,
) -> None:
    """Render portfolio composition as pie chart.

    Args:
        df_latest: Portfolio history with ticker-level positions
    """
    if df_latest.is_empty():
        return

    # get complete row of the latest date per ticker
    df_latest = df_latest.with_columns(
        pl.col("asset_type").str.to_uppercase(),
        # Fill nan in sector with ETF if asset_type is ETF else use Unknown
        pl.col("sector").fill_null(
            pl.when(pl.col("asset_type") == AssetType.ETF)
            .then(pl.lit("ETF"))
            .otherwise(pl.lit("Unknown"))
        ),
    ).sort("position_value_EUR", descending=True)
    factors = (
        pl.concat(
            [
                strategy_engine.calculate_portfolio_exposure(
                    df_latest.filter(pl.col("asset_type") == atype),
                    value_column="position_value_EUR",
                    sector_column="sector",
                ).with_columns(
                    pl.lit(atype).alias("asset_type"),
                )
                for atype in df_latest.select(pl.col("asset_type").unique()).to_series()
            ]
        )
        .filter(pl.col("value") > 0.1)
        .with_columns(
            (pl.col("value") / pl.col("value").sum()).alias("proportion"),
            pl.col("key").replace(strategy_engine.factor_mapping).alias("factor_short"),
            pl.col("key").replace(strategy_engine.factor_emoji_mapping).alias("factor_emoji"),
        )
    )

    has_group_usage = df_latest.select(pl.col("group").n_unique()).item() > 1
    n_col = 2 if has_group_usage else 1

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "Asset Classes",
            "Strategy Factors",
            "Sectors",
            "Countries",
            "Groups",
            "Positions",
        ]
    )
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
        col1, col2 = st.columns(2)
        with col1:
            fig_factor_simple = px.pie(
                factors,
                names="factor_short",
                values="proportion",
                color="factor_short",
                color_discrete_map=STRATEGY_FACTOR_COLOR_MAP,
            )
            style_pie_chart(fig_factor_simple)
            st.plotly_chart(fig_factor_simple, use_container_width=True)
        with col2:
            fig_factor = make_sunburst_chart(
                factors,
                path=["asset_type", "factor_short"],
                title="Strategy Factor Exposure by Asset Class",
                value="proportion",
            )
            st.plotly_chart(fig_factor, use_container_width=True)
    with tab3:
        tmp_stocks = df_latest.filter(pl.col("asset_type") == AssetType.STOCK.upper()).select(
            ["ticker", "group", "position_value_EUR", "sector", "asset_type"]
        )
        tmp_etfs = (
            df_etf_sectors.select(
                [
                    "ticker",
                    "group",
                    "weighted_value_EUR",
                    "category",
                ]
            )
            .rename(
                {
                    "weighted_value_EUR": "position_value_EUR",
                    "category": "sector",
                }
            )
            .with_columns(pl.lit("ETF").alias("asset_type"))
        )
        col1, col2 = st.columns(2)
        with col1:
            fig_sectors = make_pie_chart(
                pl.concat([tmp_stocks, tmp_etfs]),
                names="sector",
                values="position_value_EUR",
            )
            st.plotly_chart(fig_sectors, use_container_width=True)
        with col2:
            fig_sectors_sunburst = make_sunburst_chart(
                pl.concat([tmp_stocks, tmp_etfs]),
                path=["sector", "asset_type"],
            )
            st.plotly_chart(fig_sectors_sunburst, use_container_width=True)
    with tab4:
        tmp_stocks = df_latest.filter(pl.col("asset_type") == AssetType.STOCK.upper()).select(
            ["ticker", "group", "position_value_EUR", "country", "asset_type"]
        )
        tmp_etfs = (
            df_etf_countries.select(["ticker", "group", "weighted_value_EUR", "category"])
            .rename(
                {
                    "weighted_value_EUR": "position_value_EUR",
                    "category": "country",
                }
            )
            .with_columns(pl.lit("ETF").alias("asset_type"))
        )

        # grouping minor countries into "Other" for better visibility
        top_countries = (
            tmp_etfs.group_by("country")
            .agg(pl.sum("position_value_EUR").alias("total_value"))
            .sort("total_value", descending=True)
            .head(12)
            .select("country")
            .to_series()
            .to_list()
        )
        tmp_etfs = tmp_etfs.with_columns(
            pl.when(~pl.col("country").is_in(top_countries))
            .then(pl.lit("Other"))
            .otherwise(pl.col("country"))
            .alias("country")
        )

        col1, col2 = st.columns(2)
        with col1:
            fig_countries = make_pie_chart(
                pl.concat([tmp_stocks, tmp_etfs]),
                names="country",
                values="position_value_EUR",
            )
            st.plotly_chart(fig_countries, use_container_width=True)
        with col2:
            fig_countries_sunburst = make_sunburst_chart(
                pl.concat([tmp_stocks, tmp_etfs]),
                path=["country", "asset_type"],
            )
            st.plotly_chart(fig_countries_sunburst, use_container_width=True)
    with tab5:
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

    with tab6:
        fig_pos = make_pie_chart(
            df_latest,
            names="ticker",
            values="position_value_EUR",
        )

        st.plotly_chart(fig_pos, use_container_width=True)


def render_market_snapshot_tables(
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
            "valuation_source",
            "market_cap_b_eur",
            "pe_ratio",
            "fcf_yield",
            "roce",
            "gross_margin",
            "ebit_margin",
            "revenue_growth",
            "net_debt_to_ebit",
            "data_lag_days",
        ],
        column_config={
            "market_cap_b_eur": st.column_config.NumberColumn("Market Cap 💶", format="%.1f B€"),
            "info": "",
            "valuation_source": st.column_config.TextColumn("Source", width="small"),
            "ticker": "Ticker",
            "short_name": "Name",
            "pe_ratio": st.column_config.NumberColumn("P/E 💰", format="%.1f"),
            "fcf_yield": st.column_config.NumberColumn("FCF Yield 💰", format="%.2f%%"),
            "gross_margin": st.column_config.NumberColumn("Gross Margin 💎", format="%.1f%%"),
            "ebit_margin": st.column_config.NumberColumn("EBIT Margin 💎", format="%.1f%%"),
            "roce": st.column_config.NumberColumn("ROCE 💎", format="%.1f%%"),
            "revenue_growth": st.column_config.NumberColumn("Revenue Growth 🚀", format="%.1f%%"),
            "net_debt_to_ebit": st.column_config.NumberColumn("Debt/EBIT 🏥", format="%.1fx"),
            "data_lag_days": st.column_config.NumberColumn("Data Lag (Days) ⏱", format="%.0f"),
        },
    )


def render_strategy_factor_table(
    df_snapshot: pl.DataFrame,
    strategy_engine: StrategyEngine,
) -> None:
    if df_snapshot.is_empty():
        st.warning("No market fundamentals data to display")
        return
    df_snapshot = (
        df_snapshot.filter(
            # pl.col("asset_type") == AssetType.STOCK
        )
        .with_columns(
            (
                pl.col("country").replace(COUNTRY_FLAGS, default="❓")
                + " "
                + pl.col("sector").replace(SECTOR_EMOJI, default="👻")
            )
            # when asset class is ETF show an emoji for etf and the globe
            .alias("info")
        )
        .with_columns(
            pl.when(pl.col("asset_type") == AssetType.ETF)
            .then(
                pl.when(pl.col("name").str.to_lowercase().str.contains("europe"))
                .then(pl.lit("📑🇪🇺"))
                .otherwise(pl.lit("📑🌍"))
            )
            .otherwise(pl.col("info"))
            .alias("info")
        )
    )
    df_profile = (
        df_snapshot.select("ticker", "name", "sector", "info")
        .pipe(strategy_engine.join_factor_profiles)
        .fill_null(0.0)
        .with_columns(
            # Multiply factors by 10 for better readability
            (pl.col("tech") * 10).alias("tech"),
            (pl.col("stab") * 10).alias("stab"),
            (pl.col("real") * 10).alias("real"),
            (pl.col("price") * 10).alias("price"),
        )
    )

    st.subheader("📊 Strategy Factor Profiles")
    st.dataframe(
        df_profile,
        column_order=["name", "info", "tech", "stab", "real", "price"],
        column_config={
            "name": "Name",
            "info": "",
            "tech": st.column_config.NumberColumn("🔬 Technology", format="%.1f"),
            "stab": st.column_config.NumberColumn("🛡️ Stability", format="%.1f"),
            "real": st.column_config.NumberColumn("⚙️ Real Assets", format="%.1f"),
            "price": st.column_config.NumberColumn("👜 Pricing Power", format="%.1f"),
        },
    )
