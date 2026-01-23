import polars as pl
import streamlit as st

import src.app.views.entry as view
from src.app.logic.data_loader import load_all_stock_data
from src.app.logic.entry import calculate_volatility_metrics
from src.core.domain_models import AssetType

st.set_page_config(
    page_title="LLM Context Export",
    page_icon="🤖",
    layout="wide",
)
st.title("🤖 LLM Context Export")

dashboard_data, portfolio_dict, fx_engine = load_all_stock_data()
filter_portfolios = st.sidebar.multiselect(
    "Filter by Portfolios (optional)",
    options=(list(portfolio_dict.keys())),
    default=["Quality Core Holdings"],
)
base_selected_tickers: list[str] | None = None
if filter_portfolios:
    selected_tickers_set = set()
    for pf in filter_portfolios:
        selected_tickers_set.update(portfolio_dict.get(pf, []))
    base_selected_tickers = [str(t) for t in selected_tickers_set]


st.subheader("1️⃣ Select Tickers")
select_complete = st.toggle(
    "Select all tickers from the filtered portfolios",
    value=True,
)
if select_complete and base_selected_tickers is not None:
    selected_tickers = base_selected_tickers
else:
    selected_tickers = view.render_stock_selection(
        dashboard_data,
        base_selected_tickers,
    )

st.subheader("2️⃣ Select Info to Include")
info_options = [
    "Metadata",
    "Price History",
    "Valuations",
    "Fundamentals",
    "Timing Data",
]
selected_info = st.multiselect(
    "Select the types of information to include in the export:",
    options=info_options,
    default=info_options,
)

export_metadata = dashboard_data.metadata.filter(
    pl.col("ticker").is_in(selected_tickers) & (pl.col("asset_type") == AssetType.STOCK.value)
).select("ticker", "name", "sector", "industry", "country")

selected_stock_tickers = export_metadata.get_column("ticker").to_list()

raw_prices = dashboard_data.prices.filter(pl.col("ticker").is_in(selected_stock_tickers)).select(
    "ticker", "date", "low", "close", "volume"
)

last_30_day_prices = raw_prices.sort(["ticker", "date"]).group_by("ticker").tail(30)

every_28_day_prices = (
    raw_prices.sort(["ticker", "date"])
    .with_columns(
        # dummy index
        (pl.arange(0, pl.count()).over("ticker") % 28).alias("day_mod_28")
    )
    .filter(pl.col("day_mod_28") == 0)
    .drop("day_mod_28")
)

export_prices = (
    pl.concat([last_30_day_prices, every_28_day_prices]).unique().sort(["ticker", "date"])
)

export_valuations = (
    (
        dashboard_data.prices.filter(pl.col("ticker").is_in(selected_stock_tickers))
        .group_by("ticker")
        .last()
    )
    .pipe(
        fx_engine.convert_multiple_to_target,
        amount_cols=["close", "fair_value"],
        source_currency_col="currency",
    )
    .select(
        "ticker",
        "date",
        "close",
        "close_EUR",
        "fair_value",
        "fair_value_EUR",
        "pe_ratio",
        "ps_ratio",
        "fcf_yield",
        "dividend_yield",
        "forward_pe",
        "median_pe",
        "peg_ratio",
    )
)
export_fundamentals = (
    dashboard_data.fundamentals.filter(
        pl.col("ticker").is_in(selected_stock_tickers) & (pl.col("period_type") == "annual")
    )
    .sort(["ticker", "report_date"])
    .group_by("ticker")
    .last()
    .select(
        "ticker",
        "report_date",
        "currency",
        "basic_eps",
        "diluted_eps",
        "roce",
        "rotce",
        "net_debt_to_ebit",
        "net_debt_to_ebitda",
        "net_profit_margin",
        "gross_margin",
        "ebit_margin",
        "cash_conversion_ratio",
        "revenue_growth",
        "net_income_growth",
    )
)

export_timing = (
    calculate_volatility_metrics(
        df_prices=dashboard_data.prices,
        window_days=250,
        selected_tickers=selected_stock_tickers,
    )
    .group_by("ticker")
    .last()
    .pipe(
        fx_engine.convert_multiple_to_target,
        amount_cols=["close", "sma_200", "sma_50"],
        source_currency_col="currency",
    )
    .select(
        "ticker",
        "date",
        "close",
        "close_EUR",
        "sma_50",
        "sma_50_EUR",
        "std_50",
        "sma_200",
        "sma_200_EUR",
        "std_200",
        "z_score",
        "dist_200_pct",
        "vola_annual_pct",
    )
)


data_dict = {}
for info_type in selected_info:
    if info_type == "Metadata":
        data_dict["metadata"] = dict(
            description="Stock Metadata",
            data=export_metadata.to_dicts(),
        )
    elif info_type == "Price History":
        data_dict["price_history"] = dict(
            description="Price History",
            data=export_prices.to_dicts(),
        )
    elif info_type == "Valuations":
        data_dict["valuations"] = dict(
            description="Valuation Metrics",
            data=export_valuations.to_dicts(),
        )
    elif info_type == "Fundamentals":
        data_dict["fundamentals"] = dict(
            description="Fundamentals",
            data=export_fundamentals.to_dicts(),
        )
    elif info_type == "Timing Data":
        data_dict["timing_data"] = dict(
            description="Timing Data",
            data=export_timing.to_dicts(),
        )

st.subheader("3️⃣ Export Data as JSON")
st.json(data_dict)
