"""Chart rendering components for stock detail page.

Pure visualization functions using Plotly for interactive charts.
"""

from dataclasses import dataclass

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots
from views.colors import Colors

from src.analysis.fx import FXEngine
from src.core.stock_data import StockData

CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
}


@dataclass
class MetricDisplayInfo:
    label: str
    scale: float
    unit: str
    display_name: str


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


def render_pe_ratio_chart(df_price: pl.DataFrame, ticker: str) -> None:
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
            "Median P/E": Colors.orange,
            "Lower Quartile P/E": Colors.green,
            "Upper Quartile P/E": Colors.red,
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


def render_price_chart(
    df_price: pl.DataFrame, ticker: str, simple_display_mode: bool = True
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

    if simple_display_mode:
        df_price = df_price.with_columns(
            # add 200 day moving average
            pl.col("close").rolling_mean(window_size=200).alias("MA200"),
            pl.col("close").alias("Closing Price"),
        )
        fig = px.line(
            df_price,
            x="date",
            y=["Closing Price", "MA200", "fair_value"],
            title=f"{ticker} Closing Price History",
            labels={
                "close": f"Closing Price ({symbol})",
                "date": "Date",
                "fair_value": "Fair Value",
            },
            color_discrete_sequence=[
                Colors.blue,
                Colors.orange,
                Colors.green,
            ],
        )
        fig.update_layout(legend_title_text="")
        fig.update_yaxes(title_text=f"Price ({symbol})")
        st.plotly_chart(fig, use_container_width=True)
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


def render_quality_chart(stock_data: StockData, metrics: list[MetricDisplayInfo]) -> None:
    """Render fundamental metrics over time (ROCE, Margins, FCF)."""
    ticker = stock_data.ticker
    df_fund = stock_data.fundamentals
    if df_fund.is_empty():
        st.warning(f"No fundamental data available for {ticker}")
        return

    # Create tabs for different metric categories
    tab1, tab2, tab3 = st.tabs(["Capital Efficiency", "Margins", "Cash Flow"])

    currency = df_fund.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)

    df_fund = (
        df_fund.sort("date")
        .filter(pl.col("revenue").is_not_null())
        .with_columns(
            (pl.col("roce") * 100).alias("roce%"),
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
                y="roce%",
                labels={"roce%": "ROCE (%)", "date": "Date"},
                title=f"{ticker} Return on Capital Employed (ROCE)",
                color_discrete_sequence=[Colors.blue],
            )

            fig_roce.update_layout(
                template="plotly_white",
                height=400,
            )

            st.plotly_chart(fig_roce, use_container_width=True)
        else:
            st.info("ROCE data not available")
    with tab2:
        df_tmp = df_fund.select(["date", "gross_margin%", "ebit_margin%"]).unpivot(
            index="date",
            variable_name="margin_type",
            value_name="margin_value",
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
            color_discrete_map={
                "gross_margin%": Colors.blue,
                "ebit_margin%": Colors.light_blue,
            },
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
        # no legend


def render_valuation_data(stock_data: StockData) -> None:
    """Render key valuation and fundamental metrics as Streamlit metrics."""

    df_price = stock_data.prices

    latest_price_metrics = df_price.tail(1)
    yearly_price_metrics = df_price.group_by(pl.col("date").dt.year().alias("year")).agg(
        pl.mean("close").alias("close"),
        pl.mean("volume").alias("volume"),
        pl.sum("dividend").alias("dividend"),
        pl.mean("rolling_dividend_sum").alias("rolling_dividend_sum"),
        pl.mean("fcf_yield").alias("fcf_yield"),
        pl.mean("dividend_yield").alias("dividend_yield"),
        pl.mean("pe_ratio").alias("pe_ratio"),
        pl.mean("diluted_average_shares").alias("diluted_average_shares"),
    )
    st.subheader("💰 Valuation Metrics")
    col1, col2 = st.columns([1, 3])
    with col1:
        if "pe_ratio" in df_price.columns:
            st.metric(
                "Current P/E Ratio",
                f"{latest_price_metrics.select('pe_ratio').item():.1f}",
            )
        latest_fcf_yield = latest_price_metrics.select("fcf_yield").item()
        if latest_fcf_yield is not None:
            st.metric(
                "Current FCF Yield",
                f"{ latest_fcf_yield * 100:.1f}%",
            )
        st.metric(
            "Current Dividend Yield",
            f"{latest_price_metrics.select('dividend_yield').item() * 100:.1f}%",
        )
        st.metric(
            "Current Fair Value",
            f"${latest_price_metrics.select('fair_value').item():.0f}",
        )
    with col2:
        tab1, tab2, tab3 = st.tabs(["P/E Ratio", "Yield", "Dilution"])
        tmp_metrics = (
            yearly_price_metrics.select(
                "year",
                "fcf_yield",
                "dividend_yield",
                "pe_ratio",
                "diluted_average_shares",
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
                color_discrete_sequence=[Colors.blue],
            )
            st.plotly_chart(fig, use_container_width=True)
        with tab2:
            fig = px.bar(
                tmp_metrics.filter(pl.col("metric").is_in(["fcf_yield", "dividend_yield"])),
                x="year",
                y="yield",
                color="metric",
                barmode="group",
                labels={"yield": "Yield (%)", "year": "Year"},
                color_discrete_map={
                    "fcf_yield": Colors.blue,
                    "dividend_yield": Colors.orange,
                },
            )
            st.plotly_chart(fig, use_container_width=True)
        with tab3:
            fig = px.bar(
                tmp_metrics.filter(pl.col("metric") == "diluted_average_shares"),
                x="year",
                y="yield",
                labels={"yield": "Diluted Average Shares", "year": "Year"},
                color_discrete_sequence=[Colors.blue],
            )
            st.plotly_chart(fig, use_container_width=True)


def render_quality_data(stock_data: StockData) -> None:
    metrics = [
        MetricDisplayInfo("roce", 100, "%", "ROCE"),
        MetricDisplayInfo("gross_margin", 100, "%", "Gross Margin"),
        MetricDisplayInfo("ebit_margin", 100, "%", "EBIT Margin"),
        MetricDisplayInfo("free_cash_flow", 1e-9, "B", "Free Cash Flow"),
        MetricDisplayInfo("cash_conversion_ratio", 100, "%", "Cash Conversion Ratio"),
    ]
    st.subheader("💎 Quality Metrics")
    col1, col2 = st.columns([1, 3])
    with col1:
        render_quality_metrics(stock_data, metrics)
    with col2:
        render_quality_chart(stock_data, metrics)


def render_quality_metrics(stock_data: StockData, metrics: list[MetricDisplayInfo]) -> None:
    latest_fund = stock_data.fundamentals.tail(1)
    currency = stock_data.fundamentals.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)

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
        df_growth = (
            df_fund.select(["date", "revenue_growth", "net_income_growth"])
            .filter(
                pl.col("revenue_growth").is_not_null() | pl.col("net_income_growth").is_not_null()
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
                "Revenue Growth": Colors.blue,
                "Net Income Growth": Colors.orange,
            },
        )
        fig_growth.update_layout(
            template="plotly_white",
            height=400,
            legend_title_text="",
        )
        st.plotly_chart(fig_growth, use_container_width=True)


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
