import plotly.express as px
import polars as pl
import streamlit as st

import src.app.logic.dcf as logic
import src.app.views.entry as view
from src.app.logic.data_loader import DashboardData, load_all_stock_data
from src.app.logic.dcf import CurrentStockData, DCFInputs, DCFResult, DCFScenario
from src.app.views.colors import COLOR_SCALE_CONTRAST
from src.app.views.constants import CURRENCY_SYMBOLS, assign_info_emojis


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
    col1, col2 = st.columns([3, 2])
    with col2:
        sub_col_1, sub_col_2 = st.columns(2)
        with sub_col_1:
            st.metric("Current Net Profit Margin", f"{current_margin:.1f}%")
            st.metric(
                "Curr Cash Conversion",
                f"{current_data.cash_conversion_ratio*100:.1f}%",
            )
            st.metric("Median PE", f"{current_data.median_pe:.1f}")
        with sub_col_2:
            st.metric("Current Revenue Growth", f"{current_growth:.1f}%")
            st.metric(
                "Mean Cash Conversion",
                f"{current_data.mean_cash_conversion_ratio*100:.1f}%",
            )
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


input_prettify_mapping = {
    "projected_margin_5y": "5y Margin",
    "projected_margin_10y": "10y Margin",
    "projected_growth_5y": "5y Growth",
    "projected_growth_10y": "10y Growth",
    "discount_rate": "Discount Rate",
    "terminal_pe": "Terminal PE",
}


def display_dcf_matrix_index_element(ind_dict: dict[str, float], order: str = "vertical") -> None:
    if order == "vertical":
        for k, v in ind_dict.items():
            st.metric(label=input_prettify_mapping.get(k, k), value=v)
    elif order == "horizontal":
        cols = st.columns(len(ind_dict))
        for i, (k, v) in enumerate(ind_dict.items()):
            with cols[i]:
                st.metric(label=input_prettify_mapping.get(k, k), value=v)
    else:
        raise ValueError("Invalid order value")


def display_dcf_matrix(
    data: pl.DataFrame, scenario_row: DCFScenario, scenario_col: DCFScenario
) -> None:
    cols = st.columns(len(scenario_col) + 1)
    for i, col in enumerate(scenario_col):
        with cols[i + 1]:
            display_dcf_matrix_index_element(col, order="vertical")
    for i, row in enumerate(scenario_row):
        cols = st.columns(len(scenario_col) + 1)
        with cols[0]:
            display_dcf_matrix_index_element(row, order="horizontal")
        for j, _ in enumerate(scenario_col):
            value = (
                data.filter((pl.col("row_id") == i) & (pl.col("col_id") == j))
                .select("value_per_share")
                .item()
            )
            implied_pe = (
                data.filter((pl.col("row_id") == i) & (pl.col("col_id") == j))
                .select("implied_pe")
                .item()
            )
            with cols[j + 1]:
                st.metric("", f"{value:.2f}")
                st.caption(f"PE: {implied_pe:.1f}")


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


def prettify_label(var_name: str) -> str:
    return var_name.replace("_", " ").title()


def get_default_input_value(var_name: str, data: CurrentStockData) -> tuple[float, float, str]:
    # TODO: We can make this more elaborate later
    is_pe = var_name == "terminal_pe"
    step_size = 1.0 if is_pe else 0.01
    default_val = 20.0 if is_pe else 0.10
    fmt = "%.1f" if is_pe else "%.3f"
    return default_val, step_size, fmt


def display_sensitivity_analysis_input(
    data: CurrentStockData,
) -> tuple[DCFScenario, DCFScenario, dict[str, float]]:
    st.subheader("🌡️ Sensitivity Matrix Configuration")
    dcf_inputs = [
        "projected_margin_5y",
        "projected_margin_10y",
        "projected_growth_5y",
        "projected_growth_10y",
        "discount_rate",
        "terminal_pe",
    ]

    col1, col2 = st.columns(2)

    scenario1_dict: dict[str, list[float]] = {}
    scenario2_dict: dict[str, list[float]] = {}

    with col1:
        st.markdown("### X-Axis (Scenario 1)")
        s_cols1 = st.multiselect(
            options=dcf_inputs,
            label="Select variables to vary on X-Axis",
            format_func=prettify_label,
            default=["projected_growth_5y"],
        )
        n_scenarios_1 = st.number_input(
            "Number of steps", min_value=1, max_value=5, value=2, key="n_scen_1"
        )

        st.divider()
        for c in s_cols1:
            st.markdown(f"**{prettify_label(c)}**")
            cols = st.columns(n_scenarios_1)
            vals = []

            for i in range(n_scenarios_1):
                with cols[i]:
                    default_val, step_size, fmt = get_default_input_value(c, data)
                    val = st.number_input(
                        f"Step {i+1}",
                        key=f"sc1_{c}_{i+1}",
                        value=default_val + (i * step_size),
                        step=step_size,
                        format=fmt,
                    )
                    vals.append(val)
            scenario1_dict[c] = vals

    with col2:
        st.markdown("### Y-Axis (Scenario 2)")
        remaining_cols = [col for col in dcf_inputs if col not in s_cols1]
        s_cols2 = st.multiselect(
            options=remaining_cols,
            label="Select variables to vary on Y-Axis",
            format_func=prettify_label,
            default=["terminal_pe"] if "terminal_pe" in remaining_cols else [],
        )
        n_scenarios_2 = st.number_input(
            "Number of steps", min_value=1, max_value=5, value=2, key="n_scen_2"
        )

        st.divider()
        for c in s_cols2:
            st.markdown(f"**{prettify_label(c)}**")
            cols = st.columns(n_scenarios_2)
            vals = []

            default_val, step_size, fmt = get_default_input_value(c, data)

            for i in range(n_scenarios_2):
                with cols[i]:
                    val = st.number_input(
                        f"Step {i+1}",
                        key=f"sc2_{c}_{i+1}",
                        value=default_val + (i * step_size),
                        step=step_size,
                        format=fmt,
                    )
                    vals.append(val)
            scenario2_dict[c] = vals
    static_cols = [col for col in dcf_inputs if col not in s_cols1 and col not in s_cols2]

    st.subheader("Static Variables")
    static_dict = {}
    col1, _ = st.columns([1, 2])
    with col1:
        for c in static_cols:
            default_val, step_size, fmt = get_default_input_value(c, data)
            val = st.number_input(
                f"{prettify_label(c)}",
                key=f"static_{c}",
                value=default_val,
                step=step_size,
                format=fmt,
            )
            static_dict[c] = val

    return DCFScenario(scenario1_dict), DCFScenario(scenario2_dict), static_dict


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

    data = logic.extract_data(ticker, dashboard_data)
    display_relevant_data(ticker, dashboard_data, data)

    tab1, tab2 = st.tabs(["Single Scenario", "Multiple Scenarios"])
    with tab1:
        col1, col2 = st.columns([2, 1])
        with col1:
            inputs = display_input_form(data)
        result = logic.run_dcf_simulation(data, inputs)
        with col2:
            display_dcf_result(result, data)
        with st.expander("LLM ready data (for debugging and export)"):
            json_data_dict = dict(
                current_stock_data=data.__dict__,
                dcf_inputs=inputs.__dict__,
                dcf_result=result.__dict__,
            )
            st.json(json_data_dict)
    with tab2:
        s1, s2, remaining_inputs = display_sensitivity_analysis_input(data)
        df_scenarios = logic.run_sensitivity_analysis(
            data,
            base_inputs=remaining_inputs,
            s1=s1,
            s2=s2,
        )

        display_dcf_matrix(
            df_scenarios,
            scenario_row=s1,
            scenario_col=s2,
        )
