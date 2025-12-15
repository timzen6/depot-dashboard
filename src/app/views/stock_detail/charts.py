"""Chart rendering components for stock detail page.

Pure visualization functions using Plotly for interactive charts.
"""

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots
from views.colors import Colors

from src.analysis.fx import FXEngine

CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
}


def render_latest_price_info(
    df_price: pl.DataFrame,
    selected_ticker: str,
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
    st.dataframe(df_price)

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


def render_fundamental_chart(df_fund: pl.DataFrame, ticker: str) -> None:
    """Render fundamental metrics over time (ROCE, Margins, FCF).

    Args:
        df_fund: Fundamental data with columns [date, roce, free_cash_flow, ...]
        ticker: Stock ticker symbol for chart title
    """
    if df_fund.is_empty():
        st.warning(f"No fundamental data available for {ticker}")
        return

    # Create tabs for different metric categories
    tab1, tab2 = st.tabs(["Capital Efficiency", "Cash Flow"])
    currency = df_fund.select(pl.first("currency")).item()
    symbol = CURRENCY_SYMBOLS.get(currency, currency)
    df_fund = (
        df_fund.sort("date")
        .filter(pl.col("revenue").is_not_null())
        .with_columns(
            (pl.col("roce") * 100).alias("roce%"),
            (pl.col("ebit") / pl.col("revenue") * 100).alias("ebit_margin%"),
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
                color_discrete_map={True: "green", False: "red"},
            )
            st.plotly_chart(fig_fcf, use_container_width=True)
        else:
            st.info("Free Cash Flow data not available")


def render_quality_metrics_data(df_fund: pl.DataFrame, df_price: pl.DataFrame) -> None:
    """Render key valuation and fundamental metrics as Streamlit metrics."""
    latest_price_metrics = df_price.tail(1)
    yearly_price_metrics = df_price.group_by(pl.col("date").dt.year().alias("year")).agg(
        pl.mean("close").alias("close"),
        pl.mean("volume").alias("volume"),
        pl.sum("dividend").alias("dividend"),
        pl.mean("rolling_dividend_sum").alias("rolling_dividend_sum"),
        pl.mean("fcf_yield").alias("fcf_yield"),
        pl.mean("dividend_yield").alias("dividend_yield"),
    )

    st.subheader("Valuation Metrics")
    col1, col2 = st.columns([1, 3])
    with col1:
        latest_fcf_yield = latest_price_metrics.select("fcf_yield").item()
        if latest_fcf_yield is not None:
            st.metric(
                "Current FCF Yield",
                f"{ latest_fcf_yield * 100:.2f}%",
            )
        st.metric(
            "Current Dividend Yield",
            f"{latest_price_metrics.select('dividend_yield').item() * 100:.2f}%",
        )
    with col2:
        tmp_metrics = (
            yearly_price_metrics.select("year", "fcf_yield", "dividend_yield")
            # unpivot for bar chart
            .unpivot(
                index="year",
                variable_name="metric",
                value_name="yield",
            )
            # drop nulls
            .filter(pl.col("yield").is_not_null())
        )
        fig = px.bar(
            tmp_metrics,
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
        latest_fund = df_fund.tail(1)

    st.subheader("Fundamental Metrics")
    col1, col2, col3 = st.columns(3)

    with col1:
        if "roce" in df_fund.columns:
            roce = latest_fund.select("roce").item()
            if roce is not None:
                st.metric("Latest ROCE", f"{roce * 100:.2f}%")
            else:
                st.info("ROCE data not available")

    with col2:
        if "free_cash_flow" in df_fund.columns:
            fcf = latest_fund.select("free_cash_flow").item()
            if fcf is not None:
                st.metric("Latest FCF", f"${fcf / 1e9:.2f}B")

    with col3:
        if "net_debt" in df_fund.columns:
            net_debt = latest_fund.select("net_debt").item()
            if net_debt is not None:
                st.metric("Net Debt", f"${net_debt / 1e9:.2f}B")
