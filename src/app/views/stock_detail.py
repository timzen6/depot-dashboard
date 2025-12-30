"""Chart rendering components for stock detail page.

Pure visualization functions using Plotly for interactive charts.
"""

import math
from dataclasses import dataclass
from datetime import date

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots
from views.colors import (
    COLOR_SCALE_CONTRAST,
    COLOR_SCALE_GREEN_RED,
    STRATEGY_FACTOR_COLOR_MAP,
    Colors,
)

from src.analysis.fx import FXEngine
from src.app.logic.common import get_strategy_factor_profiles
from src.app.views.common import make_pie_chart, style_pie_chart
from src.app.views.constants import (
    COUNTRY_FLAGS,
    CURRENCY_SYMBOLS,
    get_sector_emoji_from_str,
)
from src.core.domain_models import AssetType, ETFComposition
from src.core.stock_data import StockData
from src.core.strategy_engine import StrategyEngine


@dataclass
class MetricDisplayInfo:
    label: str
    scale: float
    unit: str
    display_name: str


def render_factor_profile_chart(
    stock_metadata: dict[str, str],
    strategy_engine: StrategyEngine,
) -> None:
    asset_type = stock_metadata.get("asset_type", "")
    selected_ticker = stock_metadata.get("ticker", "")
    sector = stock_metadata.get("sector", "")
    if asset_type == AssetType.STOCK:
        df_strategy_factors = get_strategy_factor_profiles(
            pl.DataFrame([{"ticker": selected_ticker, "sector": sector}]),
            strategy_engine,
        )
        # Check if all factor values are equal to sector reference (no unique profile)
        tmp = df_strategy_factors.with_columns(pl.col("is_sector_reference").cast(pl.String)).pivot(  # noqa: PD010
            index=["factor"],
            values="value",
            on="is_sector_reference",
        )  # noqa: PD010
        is_identical = tmp.select((pl.col("true") == pl.col("false")).all()).item()
        if is_identical:
            st.info("No unique factor profile for this stock; using sector reference")
        df_strategy_factors = df_strategy_factors.with_columns(
            pl.when(pl.col("is_sector_reference"))
            .then(pl.lit("Sector Reference"))
            .otherwise(pl.lit("Stock Profile"))
            .alias("Profile Type"),
            pl.col("factor").replace(strategy_engine.factor_mapping),
        )
        fig_profile = px.bar(
            df_strategy_factors,
            x="factor",
            y="value",
            color="Profile Type",
            barmode="group",
            height=300,
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        fig_profile.update_yaxes(
            range=[0, 0.8],
            # hide tick values
            tickvals=[],
        )

        fig_profile.update_layout(
            title="Stock Strategy Factor Profile",
            template="plotly_white",
            legend_title_text="",
            height=250,
            xaxis_title="",
            yaxis_title="",
        )

        st.plotly_chart(fig_profile, use_container_width=True)


def render_title_section(
    ticker: str,
    metadata: dict[str, str],
    strategy_engine: StrategyEngine,
    valuation_source: str,
    data_lag_days: int,
) -> None:
    """Render the title section with ticker and company name.

    Args:
        ticker: Stock ticker symbol
        metadata: Metadata dictionary with company info
    """
    company_name = metadata.get("short_name", "")
    if not company_name:
        company_name = metadata.get("name", "")

    col1, col2 = st.columns([1, 1])
    with col1:
        st.title(f"🔍 {ticker} - {company_name}")

        asset_type = metadata.get("asset_type", "N/A")
        if asset_type != "stock":
            st.subheader(f"Asset Type: {asset_type.upper()}")
            return

        country_name = metadata.get("country", "")
        country_flag = COUNTRY_FLAGS.get(country_name, "")

        sector = metadata.get("sector") or metadata.get("sector_raw", "N/A")
        sector_emoji = get_sector_emoji_from_str(sector)

        st.subheader(
            f"{sector} {sector_emoji} | {metadata.get('industry', 'N/A')} |"
            f" {country_flag or country_name}"
        )
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            st.metric(
                value=valuation_source,
                label="Valuation Source 🏷",
                delta_color="inverse",
                delta_arrow="off",
            )
        with subcol2:
            st.metric(
                value=data_lag_days,
                label="Data Lag (Days) ⏱",
                delta_color="inverse",
                # warn if data lag is more than 180 days
                delta="  ⚠️ Warning: Data Lag  " if data_lag_days > 180 else None,
                delta_arrow="off",
            )
    with col2:
        render_factor_profile_chart(metadata, strategy_engine)


def render_latest_price_info(
    df_price: pl.DataFrame,
    fx_engine: FXEngine,
) -> None:
    """Render latest price information as key metrics.

    Args:
        df_price: Price data with columns [date, close, open, high, low, volume]
    """
    if df_price.is_empty():
        st.warning("No price data to display latest info")
        return

    latest = df_price.tail(1).to_dicts()[0]

    currency = latest.get("currency", "USD")
    symbol = CURRENCY_SYMBOLS.get(currency, currency)

    latest_val = latest["close"]

    if currency != "EUR":
        lastest_val_eur = fx_engine.convert_amount(
            date=latest["date"],
            amount=latest_val,
            source_currency=currency,
        )
    else:
        lastest_val_eur = None

    cols = st.columns(5)

    with cols[0]:
        st.metric(
            label="Latest Close",
            value=f"{latest['close']:,.2f} {symbol}",
        )
        if lastest_val_eur:
            st.metric(
                label="Latest Close (€)",
                value=f"{lastest_val_eur:,.2f} €",
            )

    with cols[1]:
        st.metric(
            label="Open",
            value=f"{latest['open']:,.2f} {symbol}",
        )

    with cols[2]:
        st.metric(
            label="High",
            value=f"{latest['high']:,.2f} {symbol}",
        )

    with cols[3]:
        st.metric(
            label="Low",
            value=f"{latest['low']:,.2f} {symbol}",
        )

    with cols[4]:
        st.metric(
            label="Volume",
            value=f"{latest['volume']:,.0f}",
        )


def render_pe_ratio_chart(
    df_price: pl.DataFrame, ticker: str, start_date: date | None = None
) -> None:
    """Render PE Ratio history chart.

    Args:
        df_price: Price data with columns [date, pe_ratio]
        ticker: Stock ticker symbol for chart title
    """
    if df_price.is_empty():
        st.warning(f"No price data available for {ticker}")
        return
    df_price = (
        df_price.select(["date", "pe_ratio"])
        .with_columns(
            pl.col("pe_ratio").median().alias("pe_ratio_median"),
            pl.col("pe_ratio").quantile(0.25).alias("pe_ratio_lower_quartile"),
            pl.col("pe_ratio").quantile(0.75).alias("pe_ratio_upper_quartile"),
        )
        .filter(pl.col("pe_ratio").is_not_null())
        .unpivot(
            index="date",
            variable_name="metric",
            value_name="value",
        )
        .with_columns(
            pl.col("metric").replace(
                {
                    "pe_ratio": "P/E Ratio",
                    "pe_ratio_median": "Median P/E",
                    "pe_ratio_lower_quartile": "Lower Quartile P/E",
                    "pe_ratio_upper_quartile": "Upper Quartile P/E",
                }
            )
        )
    )
    if start_date:
        df_price = df_price.filter(pl.col("date") >= start_date)

    fig = px.line(
        df_price,
        x="date",
        y="value",
        color="metric",
        line_dash="metric",
        title=f"{ticker} PE Ratio History",
        labels={
            "value": "P/E Ratio",
            "date": "Date",
        },
        color_discrete_map={
            "P/E Ratio": Colors.blue,
            "Median P/E": COLOR_SCALE_GREEN_RED[2],
            "Lower Quartile P/E": COLOR_SCALE_GREEN_RED[0],
            "Upper Quartile P/E": COLOR_SCALE_GREEN_RED[4],
        },
        line_dash_map={
            "P/E Ratio": "solid",
            "Median P/E": "dash",
            "Lower Quartile P/E": "dot",
            "Upper Quartile P/E": "dot",
        },
    )
    fig.update_layout(
        template="plotly_white",
        height=400,
        legend_title_text="",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_fcf_yield_chart(
    df_price: pl.DataFrame,
    ticker: str,
    fx_engine: FXEngine,
    start_date: date | None = None,
    use_log: bool = True,
) -> None:
    if (
        df_price.is_empty()
        or "fcf_yield" not in df_price.columns
        or df_price["fcf_yield"].is_null().all()
    ):
        st.warning(f"No price data available for {ticker}")
        return
    df_price = df_price.sort(["ticker", "date"]).pipe(
        fx_engine.convert_multiple_to_target,
        amount_cols=["close"],
        source_currency_col="currency",
    )

    median_yield = df_price.select(pl.col("fcf_yield").median()).item() * 100
    current_yield = df_price.tail(1).select(pl.col("fcf_yield")).item() * 100

    if start_date:
        df_price = df_price.filter(pl.col("date") >= start_date)
    if df_price.is_empty():
        st.warning(f"No price data available for {ticker} in selected date range")
        return

    # we must set ranges manually, because plotly leaves much to much space
    price_min = df_price.select(pl.col("close_EUR").min()).item()
    price_max = df_price.select(pl.col("close_EUR").max()).item()

    if use_log:
        safe_min = max(price_min, 0.01)
        y_min = math.log10(safe_min)
        y_max = math.log10(price_max)
        log_padding = (y_max - y_min) * 0.1
        y_min = y_min - log_padding
        y_max = y_max + log_padding

    else:
        padding = (price_max - price_min) * 0.1
        y_min = price_min - padding
        y_max = price_max + padding

    yield_min = df_price.select(pl.col("fcf_yield").min()).item() * 100
    yield_max = df_price.select(pl.col("fcf_yield").max()).item() * 100
    yield_range = yield_max - yield_min
    yield_padding = yield_range * 0.1

    y2_min = max(min(yield_min, median_yield) - yield_padding, 0)
    y2_max = max(yield_max + yield_padding, median_yield + yield_padding)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Lina A: Stock Price
    fig.add_trace(
        go.Scatter(
            x=df_price["date"],
            y=df_price["close_EUR"],
            name="Stock Price",
            line=dict(color=Colors.blue),
        ),
        secondary_y=False,
    )
    # Line B: FCF Yield
    fig.add_trace(
        go.Scatter(
            x=df_price["date"],
            y=df_price["fcf_yield"] * 100,  # convert to percentage
            name="FCF Yield (%)",
            line=dict(color=Colors.green),
        ),
        secondary_y=True,
    )
    # Median Line
    fig.add_hline(
        y=median_yield,
        line_dash="dot",
        line_color=Colors.amber,
        annotation_text=f"Median: {median_yield:.1f}%",
        annotation_position="bottom right",
        secondary_y=True,
    )

    # Styling & Log Scale Logic
    fig.update_layout(
        title=dict(
            text=f"💎 Valuation Radar: Price vs. FCF Yield (Current: {current_yield:.1f}%)",
            font=dict(size=16),
        ),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.1),
        margin=dict(t=80, l=10, r=10, b=10),
        height=500,
    )

    # Axes Updates
    fig.update_yaxes(
        title_text="Price",
        type="log" if use_log else "linear",
        range=[y_min, y_max],
        showgrid=False,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="FCF Yield %",
        range=[y2_min, y2_max],
        showgrid=True,
        secondary_y=True,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_price_chart(
    df_price: pl.DataFrame,
    ticker: str,
    simple_display_mode: bool,
    fx_engine: FXEngine,
    use_euro: bool = True,
    start_date: date | None = None,
) -> None:
    """Render price history with volume as candlestick chart.

    Args:
        df_price: Price data with columns [date, open, high, low, close, volume]
        ticker: Stock ticker symbol for chart title
    """
    if df_price.is_empty():
        st.warning(f"No price data available for {ticker}")
        return
    currency = df_price.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)

    if use_euro:
        df_price = fx_engine.convert_multiple_to_target(
            df_price,
            amount_cols=["close", "fair_value"],
            source_currency_col="currency",
        )

    if simple_display_mode:
        if use_euro:
            df_price = df_price.with_columns(
                # add 200 day moving average
                pl.col("close_EUR").rolling_mean(window_size=200).alias("MA200"),
                pl.col("close_EUR").alias("Closing Price"),
                pl.col("fair_value_EUR").alias("Fair Value"),
            )
        else:
            df_price = df_price.with_columns(
                # add 200 day moving average
                pl.col("close").rolling_mean(window_size=200).alias("MA200"),
                pl.col("close").alias("Closing Price"),
                pl.col("fair_value").alias("Fair Value"),
            )
        if start_date:
            df_price = df_price.filter(pl.col("date") >= start_date)
        fig = px.line(
            df_price,
            x="date",
            y=["Closing Price", "MA200", "Fair Value"],
            title=f"{ticker} Closing Price History",
            labels={
                "date": "Date",
            },
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        fig.update_layout(legend_title_text="")
        if use_euro:
            fig.update_yaxes(title_text="Price (€)")
        else:
            fig.update_yaxes(title_text=f"Price ({symbol})")
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=(
                f"{ticker}_simple_price_chart"
                if not use_euro
                else f"{ticker}_simple_price_chart_eur"
            ),
        )
        return

    # Create subplot with secondary y-axis for volume
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(f"{ticker} Price", "Volume"),
        row_heights=[0.7, 0.3],
    )
    if start_date:
        df_price = df_price.filter(pl.col("date") >= start_date)

    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df_price["date"],
            open=df_price["open"],
            high=df_price["high"],
            low=df_price["low"],
            close=df_price["close"],
            name="OHLC",
        ),
        row=1,
        col=1,
    )

    # Volume bars
    fig.add_trace(
        go.Bar(
            x=df_price["date"],
            y=df_price["volume"],
            name="Volume",
            marker_color="blue",
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        title=f"{ticker} Price History",
        xaxis_title="Date",
        yaxis_title=f"Price ({symbol})",
        template="plotly_white",
        height=600,
        showlegend=False,
        hovermode="x unified",
    )

    fig.update_xaxes(rangeslider_visible=False)

    st.plotly_chart(fig, use_container_width=True)


def render_quality_chart(df_fund: pl.DataFrame) -> None:
    """Render fundamental metrics over time (ROCE, Margins, FCF)."""
    ticker = df_fund.select(pl.first("ticker")).item() if "ticker" in df_fund.columns else "Unknown"
    if df_fund.is_empty():
        st.warning(f"No fundamental data available for {ticker}")
        return

    # Create tabs for different metric categories
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Capital Efficiency", "Margins", "Cash Flow", "Cash Conversion Ratio"]
    )

    currency = df_fund.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)

    df_fund = (
        df_fund.sort("date")
        .filter(pl.col("revenue").is_not_null())
        .with_columns(
            (pl.col("roce") * 100).alias("ROCE %"),
            (pl.col("rotce") * 100).alias("ROTCE %"),
            (pl.col("ebit_margin") * 100).alias("ebit_margin%"),
            (pl.col("gross_margin") * 100).alias("gross_margin%"),
        )
    )

    with tab1:
        # ROCE Chart
        if "roce" in df_fund.columns:
            fig_roce = px.bar(
                df_fund,
                x="date",
                y=["ROCE %", "ROTCE %"],
                labels={"date": "Date", "value": "Percentage (%)"},
                title=f"{ticker} Return on Capital Employed (ROCE / ROTCE)",
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
                barmode="group",
            )
            # set y range
            fig_roce.update_layout(
                template="plotly_white",
                height=400,
                yaxis=dict(range=[0, 100]),
                legend_title_text="",
            )

            st.plotly_chart(fig_roce, use_container_width=True)
        else:
            st.info("ROCE data not available")
    with tab2:
        df_tmp = (
            df_fund.select(["date", "gross_margin%", "ebit_margin%"])
            .rename({"gross_margin%": "Gross Margin", "ebit_margin%": "EBIT Margin"})
            .unpivot(
                index="date",
                variable_name="margin_type",
                value_name="margin_value",
            )
        )
        fig_margins = px.bar(
            df_tmp,
            x="date",
            y="margin_value",
            color="margin_type",
            barmode="group",
            labels={
                "margin_value": "Margin (%)",
                "date": "Date",
                "margin_type": "Margin Type",
            },
            title=f"{ticker} Gross and EBIT Margins",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        fig_margins.update_layout(
            template="plotly_white",
            height=400,
            legend_title_text="",
        )
        st.plotly_chart(fig_margins, use_container_width=True)

    with tab3:
        # Free Cash Flow Chart
        if "free_cash_flow" in df_fund.columns:
            tmp_fcf = (
                df_fund.select(["date", "free_cash_flow"])
                .filter(pl.col("free_cash_flow").is_not_null())
                .with_columns(pl.col("free_cash_flow").gt(0).alias("fcf_positive"))
            )
            fig_fcf = px.bar(
                tmp_fcf,
                x="date",
                y="free_cash_flow",
                color="fcf_positive",
                labels={"free_cash_flow": f"Free Cash Flow ({symbol})", "date": "Date"},
                title=f"{ticker} Free Cash Flow",
                color_discrete_map={True: Colors.green, False: Colors.red},
            )
            fig_fcf.update_layout(
                template="plotly_white",
                height=400,
                showlegend=False,
            )
            st.plotly_chart(fig_fcf, use_container_width=True)
        else:
            st.info("Free Cash Flow data not available")
    with tab4:
        if "cash_conversion_ratio" in df_fund.columns:
            fig_ccr = px.bar(
                df_fund,
                x="date",
                y="cash_conversion_ratio",
                labels={
                    "cash_conversion_ratio": "Cash Conversion Ratio",
                    "date": "Date",
                },
                title=f"{ticker} Cash Conversion Ratio",
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
            )
            fig_ccr.update_layout(
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_ccr, use_container_width=True)
        else:
            st.info("Cash Conversion Ratio data not available")


def render_valuation_data(stock_data: StockData, fx_engine: FXEngine) -> None:
    """Render key valuation and fundamental metrics as Streamlit metrics."""

    df_price = stock_data.prices
    latest_price_metrics = df_price.tail(1)
    latest_price_metrics = latest_price_metrics.pipe(
        fx_engine.convert_multiple_to_target,
        amount_cols=["rolling_dividend_sum", "fair_value"],
        source_currency_col="currency",
    )
    yearly_price_metrics = (
        df_price.pipe(
            fx_engine.convert_multiple_to_target,
            amount_cols=["close", "dividend", "fair_value"],
            source_currency_col="currency",
        )
        .group_by(pl.col("date").dt.year().alias("year"))
        .agg(
            pl.mean("close_EUR").alias("close_EUR"),
            pl.mean("volume").alias("volume"),
            pl.sum("dividend_EUR").alias("dividend_EUR"),
            pl.mean("fcf_yield").alias("fcf_yield"),
            pl.mean("dividend_yield").alias("dividend_yield"),
            pl.mean("pe_ratio").alias("pe_ratio"),
            pl.mean("diluted_average_shares").alias("diluted_average_shares"),
            pl.mean("fair_value_EUR").alias("fair_value_EUR"),
        )
    )
    st.subheader("💰 Valuation Metrics")
    col1, col2 = st.columns([1, 3])
    with col1:
        sub_col1, sub_col2 = st.columns(2)
        with sub_col1:
            if "pe_ratio" in df_price.columns:
                st.metric(
                    "Current P/E Ratio",
                    f"{latest_price_metrics.select('pe_ratio').item():.1f}",
                )
        with sub_col2:
            latest_fcf_yield = latest_price_metrics.select("fcf_yield").item()
            if latest_fcf_yield is not None:
                st.metric(
                    "Current FCF Yield",
                    f"{ latest_fcf_yield * 100:.1f}%",
                )
        with sub_col1:
            forward_pe = stock_data.metadata.get("forward_pe", None)
            if forward_pe is not None:
                st.metric(
                    "Forward P/E Ratio",
                    f"{forward_pe:.1f}",
                )
        with sub_col2:
            if "fair_value" in latest_price_metrics.columns:
                st.metric(
                    "Current Fair Value",
                    f"{latest_price_metrics.select('fair_value_EUR').item():.0f} €",
                )
        with sub_col1:
            st.metric(
                "Current Dividend Yield",
                f"{latest_price_metrics.select('dividend_yield').item() * 100:.1f}%",
            )
        with sub_col2:
            st.metric(
                "Current Dividend (Rolling 12M)",
                f"{latest_price_metrics.select('rolling_dividend_sum_EUR').item():.2f} €",
            )
    with col2:
        tab1, tab2, tab3, tab4 = st.tabs(["P/E Ratio", "Yield", "Dilution", "Dividends"])
        tmp_metrics = (
            yearly_price_metrics.select(
                "year",
                "fcf_yield",
                "dividend_yield",
                "pe_ratio",
                "diluted_average_shares",
                "dividend_EUR",
            )
            .with_columns(
                (pl.col("fcf_yield") * 100).alias("fcf_yield"),
                (pl.col("dividend_yield") * 100).alias("dividend_yield"),
            )
            # unpivot for bar chart
            .unpivot(
                index="year",
                variable_name="metric",
                value_name="yield",
            )
            # drop nulls
            .filter(pl.col("yield").is_not_null())
        )
        with tab1:
            fig = px.bar(
                tmp_metrics.filter(pl.col("metric") == "pe_ratio"),
                x="year",
                y="yield",
                labels={"yield": "P/E Ratio", "year": "Year"},
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
            )
            st.plotly_chart(fig, use_container_width=True)
        with tab2:
            fig = px.bar(
                tmp_metrics.filter(
                    pl.col("metric").is_in(["fcf_yield", "dividend_yield"])
                ).with_columns(
                    pl.col("metric").replace(
                        {
                            "fcf_yield": "FCF Yield",
                            "dividend_yield": "Dividend Yield",
                        }
                    )
                ),
                x="year",
                y="yield",
                color="metric",
                barmode="group",
                labels={
                    "yield": "Yield (%)",
                    "year": "Year",
                },
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
            )
            fig.update_layout(legend_title_text="")
            st.plotly_chart(fig, use_container_width=True)
        with tab3:
            fig = px.bar(
                tmp_metrics.filter(pl.col("metric") == "diluted_average_shares"),
                x="year",
                y="yield",
                labels={"yield": "Diluted Average Shares", "year": "Year"},
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
            )
            st.plotly_chart(fig, use_container_width=True)
        with tab4:
            fig = px.bar(
                tmp_metrics.filter(pl.col("metric") == "dividend_EUR"),
                x="year",
                y="yield",
                labels={"yield": "Dividend Amount (€)", "year": "Year"},
                color_discrete_sequence=COLOR_SCALE_CONTRAST,
            )
            st.plotly_chart(fig, use_container_width=True)


def render_quality_data(stock_data: StockData, fx_engine: FXEngine) -> None:
    metrics_col1 = [
        MetricDisplayInfo("roce", 100, "%", "ROCE"),
        MetricDisplayInfo("gross_margin", 100, "%", "Gross Margin"),
        MetricDisplayInfo("free_cash_flow_EUR", 1e-9, "B", "Free Cash Flow"),
    ]
    metrics_col2 = [
        MetricDisplayInfo("rotce", 100, "%", "ROTCE"),
        MetricDisplayInfo("ebit_margin", 100, "%", "EBIT Margin"),
        MetricDisplayInfo("cash_conversion_ratio", 100, "%", "Cash Conversion Ratio"),
    ]
    df_fund = stock_data.fundamentals.pipe(
        fx_engine.convert_multiple_to_target,
        amount_cols=["free_cash_flow"],
        source_currency_col="currency",
    )

    st.subheader("💎 Quality Metrics")
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        render_quality_metrics(df_fund, metrics_col1)
    with col2:
        render_quality_metrics(df_fund, metrics_col2)
    with col3:
        render_quality_chart(df_fund)


def render_quality_metrics(df_fund: pl.DataFrame, metrics: list[MetricDisplayInfo]) -> None:
    latest_fund = df_fund.tail(1)
    # all values should be in EUR for display
    symbol = "€"

    for metric in metrics:
        label = metric.display_name
        scale = metric.scale
        unit = metric.unit
        if metric.label in latest_fund.columns:
            value = latest_fund.select(metric.label).item()
            if value is not None:
                if unit == "%":
                    display_value = f"{value * scale:.2f}{unit}"
                else:
                    display_value = f"{value * scale:.2f}{unit} {symbol}"
                st.metric(label, display_value)


def render_growth_data(stock_data: StockData) -> None:
    """Render growth metrics over time."""
    ticker = stock_data.ticker
    df_fund = stock_data.fundamentals
    currency = df_fund.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)
    if df_fund.is_empty():
        st.warning(f"No fundamental data available for {ticker}")
        return

    st.subheader("🚀 Growth Metrics")
    col1, col2 = st.columns([1, 4])
    with col1:
        latest_fund = df_fund.tail(1)
        st.metric(
            "Latest Revenue Growth",
            f"{latest_fund.select('revenue_growth').item()*100:.2f}%",
        )
        st.metric(
            "Latest Net Income Growth",
            f"{latest_fund.select('net_income_growth').item()*100:.2f}%",
        )
    with col2:
        tab1, tab2 = st.tabs(["Growth Metrics", "Total Revenue & Net Income"])
        with tab1:
            df_growth = (
                df_fund.select(["date", "revenue_growth", "net_income_growth"])
                .filter(
                    pl.col("revenue_growth").is_not_null()
                    | pl.col("net_income_growth").is_not_null()
                )
                .unpivot(
                    index="date",
                    variable_name="metric",
                    value_name="value",
                )
                .with_columns(
                    pl.col("metric").replace(
                        {
                            "revenue_growth": "Revenue Growth",
                            "net_income_growth": "Net Income Growth",
                        }
                    ),
                    (pl.col("value") * 100).alias("value"),
                )
            )

            # Growth Metrics
            fig_growth = px.bar(
                df_growth,
                x="date",
                y="value",
                color="metric",
                title=f"{ticker} Growth Metrics Over Time",
                labels={"value": "Growth (%)", "date": "Date", "variable": "Metric"},
                barmode="group",
                color_discrete_map={
                    "Revenue Growth": COLOR_SCALE_CONTRAST[0],
                    "Net Income Growth": COLOR_SCALE_CONTRAST[1],
                },
            )
            fig_growth.update_layout(
                template="plotly_white",
                height=400,
                legend_title_text="",
            )
            st.plotly_chart(fig_growth, use_container_width=True)
        with tab2:
            df_revenue_income = (
                df_fund.select(["date", "revenue", "net_income"])
                .filter(pl.col("revenue").is_not_null() | pl.col("net_income").is_not_null())
                .unpivot(
                    index="date",
                    variable_name="metric",
                    value_name="value",
                )
                .with_columns(
                    pl.col("metric").replace(
                        {
                            "revenue": "Total Revenue",
                            "net_income": "Net Income",
                        }
                    ),
                    (1e-9 * pl.col("value")).alias("value"),
                )
            )

            # Total Revenue & Net Income
            fig_revenue_income = px.bar(
                df_revenue_income,
                x="date",
                y="value",
                color="metric",
                title=f"{ticker} Total Revenue & Net Income Over Time",
                labels={
                    "value": f"Amount B({symbol})",
                    "date": "Date",
                    "variable": "Metric",
                },
                barmode="group",
                color_discrete_map={
                    "Total Revenue": Colors.blue,
                    "Net Income": Colors.orange,
                },
            )
            fig_revenue_income.update_layout(
                template="plotly_white",
                height=400,
                legend_title_text="",
            )
            st.plotly_chart(fig_revenue_income, use_container_width=True)


def render_health_data(stock_data: StockData) -> None:
    """Render health metrics over time."""
    st.subheader("🏥 Health Metrics")
    df_fund = stock_data.fundamentals
    ticker = stock_data.ticker
    currency = df_fund.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)
    latest_fund = df_fund.tail(1)
    col1, col2 = st.columns([1, 4])
    with col1:
        st.metric(
            "Latest Net Debt",
            f"{latest_fund.select('net_debt').item()/1e6:.2f} M {symbol}",
        )
        st.metric(
            "Latest Net Debt to EBIT",
            f"{latest_fund.select('net_debt_to_ebit').item():.2f}",
        )
    with col2:
        tab1, tab2 = st.tabs(["Net Debt", "Net Debt to EBIT"])
        with tab1:
            fig_net_debt = px.bar(
                df_fund.drop_nulls("net_debt"),
                x="date",
                y="net_debt",
                labels={"net_debt": f"Net Debt ({symbol})", "date": "Date"},
                title=f"{ticker} Net Debt Over Time",
                color_discrete_sequence=[Colors.blue],
            )
            fig_net_debt.update_layout(
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_net_debt, use_container_width=True)
        with tab2:
            fig_net_debt_ebit = px.bar(
                df_fund.drop_nulls("net_debt_to_ebit"),
                x="date",
                y="net_debt_to_ebit",
                labels={"net_debt_to_ebit": "Net Debt to EBIT", "date": "Date"},
                title=f"{ticker} Net Debt to EBIT Over Time",
                color_discrete_sequence=[Colors.blue],
            )
            fig_net_debt_ebit.update_layout(
                template="plotly_white",
                height=400,
            )
            st.plotly_chart(fig_net_debt_ebit, use_container_width=True)


def render_etf_composition_charts(
    etf_comp: ETFComposition, strategy_engine: StrategyEngine
) -> None:
    """Render ETF composition charts for sectors and countries."""
    st.subheader(f"ETF Information | TER: {etf_comp.ter}% | Strategy: {etf_comp.strategy}")
    col1, col2, col3 = st.columns(3)
    with col1:
        s_fig = make_pie_chart(
            etf_comp.sectors_df,
            names="category",
            values="weight",
            title="Sector Allocation",
        )
        st.plotly_chart(s_fig, use_container_width=True)
        holdings_df = etf_comp.top_holdings_df.select(
            [
                pl.col("holding_name").alias("name"),
                (pl.col("weight") * 100).round(2).alias("weight_%"),
            ]
        )
        st.markdown("### Top Holdings")
        st.dataframe(
            holdings_df,
            column_order=["name", "weight_%"],
            column_config={
                "name": "Holding Name",
                "weight_%": "Weight (%)",
            },
        )
    with col2:
        c_fig = make_pie_chart(
            etf_comp.countries_df,
            names="category",
            values="weight",
            title="Country Allocation",
        )
        st.plotly_chart(c_fig, use_container_width=True)
    with col3:
        direct_estimate = strategy_engine.get_factor_profile(etf_comp.ticker, sector="ETF")

        factor_mapping = strategy_engine.factor_mapping
        factor_mapping["unclassified"] = "Unclassified"

        df_direct = (
            pl.DataFrame(direct_estimate.to_dict())
            .unpivot(
                variable_name="metric",
                value_name="value",
            )
            .with_columns(
                pl.col("metric").replace(factor_mapping),
            )
        )

        fig_direct = px.pie(
            df_direct,
            names="metric",
            values="value",
            title="Strategy Factor Exposure (direct estimate)",
            color="metric",
            color_discrete_map=STRATEGY_FACTOR_COLOR_MAP,
        )
        style_pie_chart(fig_direct)
        st.plotly_chart(fig_direct, use_container_width=True)

        st.caption(
            "Usually the direct estimate should better capture the "
            "actual factor exposure of the ETF."
            " However, the sector-weighted exposure is provided for comparison."
        )

        sector_df = etf_comp.sectors_df.with_columns(pl.lit("dummy").alias("ticker"))
        factors = strategy_engine.calculate_portfolio_exposure(
            sector_df,
            value_column="weight",
            sector_column="category",
        ).with_columns(
            # for all factor names map Real Assetes / Industry to Real Assets
            pl.col("key").replace(factor_mapping).alias("factor"),
        )
        fig_factor = px.pie(
            factors,
            names="factor",
            values="proportion",
            color="factor",
            title="Strategy Factor Exposure (weighted sector allocation)",
            color_discrete_map=STRATEGY_FACTOR_COLOR_MAP,
        )
        style_pie_chart(fig_factor)
        st.plotly_chart(fig_factor, use_container_width=True)


def render_fundamentals_reference(df_fund: pl.DataFrame) -> None:
    """Render key fundamental metrics as Reference for Debugging."""
    raw_fundamentals = (
        df_fund.select(
            [
                # fmt: off
                "ticker",
                "report_date",
                "revenue",
                "gross_profit",
                "ebit",
                "net_income",
                "tax_provision",
                "interest_expense",
                "diluted_eps",
                "basic_eps",
                "operating_cash_flow",
                "capital_expenditure",
                "free_cash_flow",
                "cash_dividends_paid",
                "basic_average_shares",
                "diluted_average_shares",
                "share_issued",
                "total_assets",
                "total_current_liabilities",
                "total_equity",
                "long_term_debt",
                "short_term_debt",
                "cash_and_equivalents",
                "total_debt",
                "goodwill",
                "intangible_assets",
                "goodwill_and_intangible_assets",
                # fmt: on
            ]
        )
        .unpivot(
            index=[
                "report_date",
            ],
            variable_name="metric",
            value_name="value",
        )
        .sort(["report_date", "metric"], descending=[True, False])
    )
    with st.expander("Show Raw Fundamental Data"):
        st.dataframe(
            raw_fundamentals,
            use_container_width=True,
        )
