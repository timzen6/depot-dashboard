import polars as pl
import streamlit as st

import src.app.logic.entry as logic
import src.app.views.entry as view
from src.app.logic.data_loader import load_all_stock_data

# --- CONFIG ---
st.set_page_config(
    page_title="Entry Level Analysis",
    page_icon="🚪",
    layout="wide",
)

st.title("🚪 Entry Level Analysis")

dashboard_data, portfolio_dict, fx_engine = load_all_stock_data()
cutoff_date, window_days, portfolio_tickers = view.render_sidebar_selection(
    dashboard_data, portfolio_dict
)

selected_tickers = view.render_stock_selection(
    dashboard_data,
    portfolio_tickers,
)
if not selected_tickers:
    st.info("Please select at least one ticker from the table above.")
    st.stop()

# --- ENGINE: DATA PROCESSING ---

selected_price_data = logic.calculate_volatility_metrics(
    df_prices=dashboard_data.prices,
    window_days=window_days,
    selected_tickers=selected_tickers,
).pipe(fx_engine.convert_to_target, "close", source_currency_col="currency")

df_status, ticker_corridors = logic.calculate_ticker_status(
    df_data=selected_price_data,
    selected_tickers=selected_tickers,
)

st.subheader("2️⃣ Tactical & Strategic Status Overview")
if not df_status.is_empty():
    view.render_tactic_strategic_overview_table(df_status)

selected_price_data = selected_price_data.filter(pl.col("date") >= cutoff_date)
st.subheader("3️⃣ Charts: Trend & Deviation")
chart_view_toggle = st.toggle(
    "View Comprehensive Trend Analysis Charts",
    value=False,
)

if chart_view_toggle:
    view.render_trend_analysis_charts(
        selected_price_data,
        selected_tickers,
        ticker_corridors,
    )

st.divider()
st.subheader("🎯 Recommended Limit Orders")

with st.expander("ℹ️ How to read these recommendations?", expanded=True):
    st.markdown(
        f"""
        These limits are based on **historical fill probability** over the selected timeframe.

        Currently selected: **Last {window_days} days**

        * 🛡️ **Safe (90% Prob):** Historically,
        this discount level was reached in **90%** of all weeks/months.
            *Use this if you want to ensure entry.*
        * ⚖️ **Balanced (50% Prob):** The median discount. A "coin flip" chance of getting filled.
        * 💰 **Aggressive (25% Prob):** A larger discount that only happens in volatile weeks.
            *Use this for speculative "stink bids" or if Valuation Rank is very high.*
        """
    )

col_opt1, col_opt2 = st.columns([1, 3])
with col_opt1:
    show_in_eur = st.toggle("Show all prices in EUR", value=False)

df_limits = logic.calculate_limit_recommendation_data(
    selected_price_data=selected_price_data,
    df_status=df_status,
    selected_tickers=selected_tickers,
    show_in_eur=show_in_eur,
)
if not df_limits.is_empty():
    df_display = df_limits.to_pandas()
    st.dataframe(
        df_display.style.applymap(view.highlight_valuation_ranks, subset=["valuation_rank"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker"),
            "valuation_rank": st.column_config.ProgressColumn(
                "Valuation Rank", format="%.2f", min_value=0, max_value=1
            ),
            "z_score": st.column_config.NumberColumn("Tactical Z", format="%.1f"),
            "current": st.column_config.TextColumn("Current Price"),
            "safe": st.column_config.TextColumn("🛡️ Safe (90%)"),
            "balanced": st.column_config.TextColumn("⚖️ Balanced (50%)"),
            "aggressive": st.column_config.TextColumn("💰 Aggressive (25%)"),
        },
    )
else:
    st.warning("Not enough historical data to calculate reliable limits.")
