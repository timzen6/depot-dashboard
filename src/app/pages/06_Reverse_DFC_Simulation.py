from dataclasses import dataclass

import plotly.express as px
import polars as pl
import streamlit as st

import src.app.views.entry as view
from src.app.logic.data_loader import DashboardData, load_all_stock_data
from src.app.views.colors import COLOR_SCALE_CONTRAST
from src.app.views.constants import CURRENCY_SYMBOLS, assign_info_emojis
from src.core.domain_models import Sector

# import colors


PRICE_COLS = [
    "ticker",
    "date",
    "close",
    "currency",
    "pe_ratio",
    "forward_pe",
    "median_pe",
]
FUND_COLS = [
    "ticker",
    "date",
    "currency",
    "revenue",
    "net_income",
    "free_cash_flow",
    "diluted_average_shares",
    "roce",
    "net_profit_margin",
    "ebit_margin",
    "revenue_growth",
    "diluted_eps",
]


@dataclass
class DCFResult:
    n_year: int
    total_value: float
    total_value_per_share: float
    terminal_value: float
    terminal_value_per_share: float


@dataclass
class CurrentStockData:
    ticker: str
    currency: str
    revenue: float
    net_income: float
    earnings_per_share: float
    free_cash_flow: float
    diluted_average_shares: float
    profit_margin: float
    revenue_growth: float
    mean_revenue_growth: float
    current_price: float
    current_pe: float
    median_pe: float
    sector_pe: float


@dataclass
class DCFInputs:
    projected_margin_5y: float
    projected_margin_10y: float
    projected_growth_5y: float
    projected_growth_10y: float
    discount_rate: float
    terminal_pe: float


def display_selection_headline(ticker: str, metadata: pl.DataFrame) -> None:
    company_row = metadata.filter(pl.col("ticker") == ticker).pipe(
        assign_info_emojis,
    )

    company_name = company_row.select("name").item()
    info_emoji = company_row.select("info").item()
    st.header(f"Simulation for {company_name} ({ticker}) {info_emoji}")


def display_relevant_data(ticker: str, data: DashboardData, current_data: CurrentStockData) -> None:
    currency = current_data.currency
    currency_symbol = CURRENCY_SYMBOLS.get(currency, currency)

    fund = (
        data.fundamentals.filter((pl.col("ticker") == ticker) & (pl.col("period_type") == "annual"))
        .select(["date", "net_profit_margin", "revenue_growth"])
        .rename(
            {
                "net_profit_margin": "Net Profit Margin",
                "revenue_growth": "Revenue Growth",
            }
        )
        .unpivot(
            index="date",
            variable_name="metric",
            value_name="value",
        )
        .with_columns(pl.col("value") * 100)
        .sort(["metric", "date"])
    )
    current_margin = (
        fund.filter(pl.col("metric") == "Net Profit Margin").select("value").tail(1).item()
    )
    current_growth = (
        fund.filter(pl.col("metric") == "Revenue Growth").select("value").tail(1).item()
    )
    col1, col2 = st.columns([3, 1])
    with col2:
        st.metric("Current Net Profit Margin", f"{current_margin:.1f}%")
        st.metric("Current Revenue Growth", f"{current_growth:.1f}%")
        sub_col_1, sub_col_2 = st.columns(2)
        with sub_col_1:
            st.metric("Median PE", f"{current_data.median_pe:.1f}")
        with sub_col_2:
            st.metric("Current PE", f"{current_data.current_pe:.1f}")
        st.metric("Sector PE", f"{current_data.sector_pe:.1f}")
        st.metric("Current Price", f"{current_data.current_price:.2f} {currency_symbol}")
    with col1:
        fig = px.bar(
            fund,
            x="date",
            y="value",
            color="metric",
            barmode="group",
            color_discrete_sequence=COLOR_SCALE_CONTRAST,
        )
        fig.update_layout(
            legend_title_text="",
            xaxis_title="Date",
            yaxis_title="Value (%)",
        )
        st.plotly_chart(fig)


def run_dcf_simulation(data: CurrentStockData, inputs: DCFInputs) -> DCFResult:
    initial_data = []
    for year in range(1, 11):
        if year <= 5:
            rate = inputs.projected_growth_5y
            margin = (
                inputs.projected_margin_5y - data.profit_margin
            ) / 5 * year + data.profit_margin
        else:
            rate = ((inputs.projected_growth_10y - inputs.projected_growth_5y) / 5) * (
                year - 5
            ) + inputs.projected_growth_5y
            margin = ((inputs.projected_margin_10y - inputs.projected_margin_5y) / 5) * (
                year - 5
            ) + inputs.projected_margin_5y
        initial_data.append(
            {
                "year": year,
                "rate": rate,
                "margin": margin,
            }
        )

    revenue = (1 + pl.col("rate")).cum_prod() * data.revenue
    profit = revenue * pl.col("margin")
    df_dcf = pl.DataFrame(initial_data).with_columns(
        revenue.alias("revenue"),
        profit.alias("net_income"),
        # discounted profit
        (profit / ((1 + inputs.discount_rate) ** pl.col("year"))).alias("discounted_net_income"),
    )
    terminal_value = (
        df_dcf.select(pl.col("net_income").last()).item()
        * inputs.terminal_pe
        / ((1 + inputs.discount_rate) ** 10)
    )

    total_value = df_dcf.select(pl.col("discounted_net_income").sum()).item() + terminal_value

    result = DCFResult(
        n_year=10,
        total_value=total_value,
        total_value_per_share=total_value / data.diluted_average_shares,
        terminal_value=terminal_value,
        terminal_value_per_share=terminal_value / data.diluted_average_shares,
    )
    return result


def display_dcf_result(result: DCFResult, data: CurrentStockData) -> None:
    currency_symbol = CURRENCY_SYMBOLS.get(data.currency, data.currency)
    st.subheader("DCF Simulation Result")
    st.metric("Value per Share", f"{result.total_value_per_share:.2f} {currency_symbol}")
    st.caption(f"Implied PE Ratio: {result.total_value_per_share / data.earnings_per_share:.1f}")
    st.metric(
        "Terminal Value per Share",
        f"{result.terminal_value_per_share:.2f} {currency_symbol}",
    )
    st.caption(
        f"""
        Relative Terminal Value:
        {result.terminal_value_per_share / result.total_value_per_share:.2%}"
        """
    )
    st.metric("Total Company Value", f"{result.total_value / 1e9:.2f}B {currency_symbol}")


sector_pe_mapping = {
    Sector.TECHNOLOGY.value: 22,
    Sector.HEALTHCARE.value: 20,
    Sector.FINANCIALS.value: 12,
    Sector.CONSUMER_DISCRETIONARY.value: 20,
    Sector.CONSUMER_STAPLES.value: 18,
    Sector.ENERGY.value: 12,
    Sector.INDUSTRIALS.value: 18,
    Sector.MATERIALS.value: 15,
    Sector.UTILITIES.value: 12,
    Sector.REAL_ESTATE.value: 15,
    Sector.COMMUNICATION.value: 20,
}


def display_input_form(data: CurrentStockData) -> DCFInputs:
    st.subheader("DCF Simulation Inputs")
    margin_max = 80.0
    margin_step = 0.5

    growth_step = 0.5

    projected_margin_5y = st.slider(
        "Projected Profit Margin in 5 Years",
        format="%.1f%%",
        min_value=0.0,
        max_value=margin_max,
        value=float(round(data.profit_margin * 100 + 5)),
        step=margin_step,
    )
    projected_margin_10y = st.slider(
        "Projected Profit Margin in 10 Years",
        format="%.1f%%",
        min_value=0.0,
        max_value=margin_max,
        value=float(round(data.profit_margin * 100)),
        step=margin_step,
    )
    projected_growth_5y = st.slider(
        "Projected Revenue Growth Rate in 5 Years",
        format="%.1f%%",
        min_value=-10.0,
        max_value=30.0,
        value=float(max(round(data.mean_revenue_growth * 100), 0)),
        step=growth_step,
    )
    projected_growth_10y = st.slider(
        "Projected Revenue Growth Rate in 10 Years",
        format="%.1f%%",
        min_value=-10.0,
        max_value=30.0,
        value=float(max(round(data.mean_revenue_growth * 100), 0)),
        step=growth_step,
    )
    discount_rate = st.slider(
        "Discount Rate",
        format="%.1f%%",
        min_value=1.0,
        max_value=20.0,
        value=10.0,
        step=0.5,
    )
    terminal_pe = st.slider(
        "Terminal PE Ratio",
        min_value=5,
        max_value=50,
        value=data.sector_pe or data.median_pe or 15,
        step=1,
    )

    return DCFInputs(
        projected_margin_5y=projected_margin_5y / 100,
        projected_margin_10y=projected_margin_10y / 100,
        projected_growth_5y=projected_growth_5y / 100,
        projected_growth_10y=projected_growth_10y / 100,
        discount_rate=discount_rate / 100,
        terminal_pe=terminal_pe,
    )


def extract_data(ticker: str, data: DashboardData) -> CurrentStockData:
    sector = data.metadata.filter(pl.col("ticker") == ticker).select("sector").item()
    sector_pe = sector_pe_mapping.get(sector, 15)
    prices_last = (
        data.prices.filter(pl.col("ticker") == ticker).sort("date").tail(1).select(PRICE_COLS)
    )
    current_price = prices_last.select("close").item()
    mean_revenue_growth = (
        data.fundamentals.filter((pl.col("ticker") == ticker) & (pl.col("period_type") == "annual"))
        .select("revenue_growth")
        .filter(pl.col("revenue_growth").is_not_null())
        .select(pl.col("revenue_growth").mean())
        .item()
    )
    fundamentals_last = (
        data.fundamentals.filter((pl.col("ticker") == ticker) & (pl.col("period_type") == "annual"))
        .sort("date")
        .tail(1)
        .select(FUND_COLS)
    )
    median_pe = prices_last.select("median_pe").item()
    return_data = CurrentStockData(
        ticker=ticker,
        currency=fundamentals_last.select("currency").item(),
        revenue=fundamentals_last.select("revenue").item(),
        net_income=fundamentals_last.select("net_income").item(),
        free_cash_flow=fundamentals_last.select("free_cash_flow").item(),
        diluted_average_shares=fundamentals_last.select("diluted_average_shares").item(),
        profit_margin=fundamentals_last.select("net_profit_margin").item(),
        revenue_growth=fundamentals_last.select("revenue_growth").item(),
        mean_revenue_growth=mean_revenue_growth,
        earnings_per_share=fundamentals_last.select("diluted_eps").item(),
        current_price=current_price,
        current_pe=prices_last.select("pe_ratio").item(),
        median_pe=median_pe,
        sector_pe=sector_pe,
    )
    return return_data


st.set_page_config(
    page_title="Reverse DFC Simulation",
    page_icon="⚙️",
    layout="wide",
)
st.title("⚙️ Reverse DFC Simulation")

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

selected_tickers = view.render_stock_selection(
    dashboard_data,
    base_selected_tickers,
    multi_select=False,
)
if selected_tickers:
    ticker = selected_tickers[0]
    display_selection_headline(ticker, dashboard_data.metadata)

    data = extract_data(ticker, dashboard_data)
    display_relevant_data(ticker, dashboard_data, data)

    simulation_type = st.selectbox(
        "Select Simulation Method",
        options=["Single Scenario", "Multiple Scenarios (coming soon)"],
    )

    if simulation_type == "Single Scenario":
        col1, col2 = st.columns([2, 1])
        with col1:
            inputs = display_input_form(data)
        result = run_dcf_simulation(data, inputs)
        with col2:
            display_dcf_result(result, data)
        with st.expander("LLM ready data (for debugging and export)"):
            json_data_dict = dict(
                current_stock_data=data.__dict__,
                dcf_inputs=inputs.__dict__,
                dcf_result=result.__dict__,
            )
            st.json(json_data_dict)
    else:
        st.info("Multiple Scenarios simulation is coming soon!")
