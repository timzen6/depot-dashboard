from collections.abc import Generator
from dataclasses import dataclass

import polars as pl

from src.app.logic.data_loader import DashboardData
from src.core.domain_models import Sector


class DCFScenario:
    def __init__(self, scenario_dict: dict[str, list[float]]):
        self.data = scenario_dict
        self.keys = list(scenario_dict.keys())
        self.values = list(zip(*scenario_dict.values(), strict=False))

    def __len__(self) -> int:
        return len(self.values)

    def get_input(self, index: int) -> dict[str, float]:
        if index < 0 or index >= len(self):
            raise IndexError("Scenario index out of range")
        return {k: v[index] for k, v in self.data.items()}

    def get_var_records(self) -> list[tuple[str, float]]:
        return list(zip(*self.data.values(), strict=False))

    def __iter__(self) -> Generator[dict[str, float], None, None]:
        for i in range(len(self)):
            yield {k: v[i] for k, v in self.data.items()}


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
    cash_conversion_ratio: float
    mean_cash_conversion_ratio: float
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
    "cash_conversion_ratio",
]


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
    mean_cash_conversion_ratio = (
        data.fundamentals.filter((pl.col("ticker") == ticker) & (pl.col("period_type") == "annual"))
        .select("cash_conversion_ratio")
        .filter(pl.col("cash_conversion_ratio").is_not_null())
        .select(pl.col("cash_conversion_ratio").mean())
        .item()
    )
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
        cash_conversion_ratio=fundamentals_last.select("cash_conversion_ratio").item(),
        mean_cash_conversion_ratio=mean_cash_conversion_ratio,
    )
    return return_data


def run_dcf_simulation(data: CurrentStockData, inputs: DCFInputs) -> DCFResult:
    df_init = pl.DataFrame(
        {
            "year": list(range(1, 11)),
        }
    ).with_columns(
        rate=pl.when(pl.col("year") <= 5)
        .then(inputs.projected_growth_5y)
        .otherwise(
            (inputs.projected_growth_10y - inputs.projected_growth_5y) / 5 * (pl.col("year") - 5)
            + inputs.projected_growth_5y
        ),
        margin=pl.when(pl.col("year") <= 5)
        .then(
            (inputs.projected_margin_5y - data.profit_margin) / 5 * pl.col("year")
            + data.profit_margin
        )
        .otherwise(
            (inputs.projected_margin_10y - inputs.projected_margin_5y) / 5 * (pl.col("year") - 5)
            + inputs.projected_margin_5y
        ),
    )

    revenue = (1 + pl.col("rate")).cum_prod() * data.revenue
    profit = revenue * pl.col("margin")
    df_dcf = df_init.with_columns(
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


def run_sensitivity_analysis(
    data: CurrentStockData,
    base_inputs: dict[str, float],
    s1: DCFScenario,
    s2: DCFScenario,
) -> pl.DataFrame:
    scenarios = []
    for i in range(len(s1)):
        for j in range(len(s2)):
            input_dict = {
                **s1.get_input(i),
                **s2.get_input(j),
                **base_inputs,
            }

            test_input = DCFInputs(
                **input_dict,
            )
            result = run_dcf_simulation(data, test_input)
            scenarios.append(
                {
                    "row_id": i,
                    "col_id": j,
                    "value_per_share": result.total_value_per_share,
                    "implied_pe": result.total_value_per_share / data.earnings_per_share,
                }
            )
    df_scenarios = pl.DataFrame(scenarios)
    return df_scenarios
