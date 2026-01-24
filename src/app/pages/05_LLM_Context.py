import polars as pl
import streamlit as st

import src.app.logic.llm_context as logic
import src.app.views.entry as view
from src.app.logic.data_loader import load_all_stock_data
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

context_builder = logic.ContextBuilder(
    dashboard_data=dashboard_data,
    fx_engine=fx_engine,
)

# For stock analysis ETF are excluded
selected_stock_tickers = (
    dashboard_data.metadata.filter(
        pl.col("ticker").is_in(selected_tickers) & (pl.col("asset_type") == AssetType.STOCK.value)
    )
    .get_column("ticker")
    .to_list()
)

export_metadata = context_builder.get_metadata(selected_stock_tickers)
export_prices = context_builder.get_price_history(selected_stock_tickers)
export_valuations = context_builder.get_valuations(selected_stock_tickers)
export_fundamentals = context_builder.get_fundamentals(selected_stock_tickers)
export_timing = context_builder.get_timing(selected_stock_tickers)

data_dict = {}
for info_type in selected_info:
    if info_type == "Metadata":
        data_dict["metadata"] = export_metadata
    elif info_type == "Price History":
        data_dict["price_history"] = export_prices
    elif info_type == "Valuations":
        data_dict["valuations"] = export_valuations
    elif info_type == "Fundamentals":
        data_dict["fundamentals"] = export_fundamentals
    elif info_type == "Timing Data":
        data_dict["timing_data"] = export_timing

st.subheader("3️⃣ Export Data as JSON")
st.info(f"Number of tokens (approx.): {len(str(data_dict)) // 4}")
st.json(data_dict)
