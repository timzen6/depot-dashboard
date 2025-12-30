from typing import Any

import polars as pl
from loguru import logger

from src.analysis.fx import FXEngine
from src.analysis.portfolio import PortfolioEngine
from src.app.logic.common import COUNTRY_REGION_MAP
from src.app.logic.data_loader import DashboardData
from src.app.logic.etf import calculate_etf_weighted_exposure
from src.app.logic.portfolio import get_portfolio_kpis, get_portfolio_performance
from src.config.models import Portfolio
from src.core.domain_models import AssetType
from src.core.etf_loader import ETFLoader
from src.core.strategy_engine import StrategyEngine


def _check_watch_list_row(row: dict[str, str | int | float | None]) -> str | None:
    raw_metric = row["metric"]
    if not isinstance(raw_metric, str):
        return None

    metric = raw_metric
    if raw_metric in ["price", "price_below"]:
        raw_value = row.get("close_EUR")
    else:
        raw_value = row.get(metric)

    raw_threshold_fair = row.get("fair_threshold")
    raw_threshold_good = row.get("good_threshold")

    if raw_value is None or raw_threshold_good is None:
        return None
    if raw_threshold_fair is None:
        raw_threshold_fair = raw_threshold_good
    try:
        fair_threshold = float(raw_threshold_fair)
        good_threshold = float(raw_threshold_good)
        value = float(raw_value)
    except (TypeError, ValueError):
        return None

    gt_metrics = ["upside", "fcf_yield", "price_below"]
    lt_metrics = ["pe_ratio", "price", "forward_pe"]
    all_metrics = gt_metrics + lt_metrics
    if metric not in all_metrics:
        logger.warning(f"Unknown metric '{metric}' in watch list alert.")
        return None

    action = row.get("action")
    if action is None:
        return None

    if action == "buy":
        if metric in gt_metrics:
            if value >= good_threshold:
                return f"GOOD: {metric} > {good_threshold}"
            if value >= fair_threshold:
                return f"FAIR: {metric} > {fair_threshold}"
        if metric in lt_metrics:
            if value <= good_threshold:
                return f"GOOD: {metric} < {good_threshold}"
            if value <= fair_threshold:
                return f"FAIR: {metric} < {fair_threshold}"
    if action == "sell":
        if metric in gt_metrics:
            if value <= good_threshold:
                return f"GOOD: {metric} < {good_threshold}"
            if value <= fair_threshold:
                return f"FAIR: {metric} < {fair_threshold}"
        if metric in lt_metrics:
            if value >= good_threshold:
                return f"GOOD: {metric} > {good_threshold}"
            if value >= fair_threshold:
                return f"FAIR: {metric} > {fair_threshold}"

    return None


def check_watch_list(
    df_latest: pl.DataFrame,
    watch_list: list[dict[str, Any]],
    fx_engine: FXEngine,
) -> pl.DataFrame:
    """Check which assets from the watch list are present in the latest data."""

    df_latest = (
        df_latest.filter(pl.col("asset_type") == AssetType.STOCK)
        .sort(["ticker", "date"])
        .group_by("ticker")
        .last()
        .select(
            [
                "ticker",
                "name",
                "close",
                "date",
                "pe_ratio",
                "fair_value",
                "currency",
                "forward_pe",
                "fcf_yield",
            ]
        )
        .with_columns(
            (((pl.col("fair_value") / pl.col("close")) - 1) * 100).alias("upside"),
            pl.col("fcf_yield") * 100,
        )
        .pipe(
            fx_engine.convert_multiple_to_target,
            amount_cols=["close", "fair_value"],
            source_currency_col="currency",
        )
    )
    df_watch = (
        pl.DataFrame(watch_list)
        .join(df_latest, on="ticker", how="left")
        .with_columns(
            pl.struct(pl.all())
            .map_elements(_check_watch_list_row, return_dtype=pl.Utf8)
            .alias("alert")
        )
    )

    return df_watch


def calculate_multiple_portfolio_metrics(
    data: DashboardData,
    portfolios: list[Portfolio],
    fx_engine: FXEngine,
    portfolio_engine: PortfolioEngine,
    strategy_engine: StrategyEngine,
    etf_loader: ETFLoader,
) -> pl.DataFrame:
    portfolio_data = []

    for portfolio in portfolios:
        df_history = get_portfolio_performance(portfolio, data.prices, fx_engine, portfolio_engine)
        kpis = get_portfolio_kpis(df_history)
        df_latest = (
            df_history.sort(["ticker", "date"])
            .group_by("ticker")
            .last()
            .join(data.metadata, on="ticker", how="left")
        )
        factors = strategy_engine.calculate_portfolio_exposure(
            df_latest, value_column="position_value_EUR"
        )
        total_real = factors.filter(pl.col("key") == "real").select("proportion").item() * 100
        total_stab = factors.filter(pl.col("key") == "stab").select("proportion").item() * 100
        total_price = factors.filter(pl.col("key") == "price").select("proportion").item() * 100
        total_tech = factors.filter(pl.col("key") == "tech").select("proportion").item() * 100

        stock_percentage = (
            df_latest.filter(pl.col("asset_type") == AssetType.STOCK)
            .select(pl.col("position_value_EUR"))
            .sum()
            .item()
            / df_latest.select(pl.col("position_value_EUR")).sum().item()
            * 100
        )
        if df_latest.filter(pl.col("asset_type") == AssetType.ETF).height > 0:
            etf_countries = (
                calculate_etf_weighted_exposure(df_latest, etf_loader.get_all_countries())
                .drop("weight", "position_value_EUR", "country")
                .rename(
                    {
                        "weighted_value_EUR": "position_value_EUR",
                        "category": "country",
                    }
                )
                .select(["ticker", "country", "position_value_EUR", "asset_type"])
            )
        else:
            etf_countries = pl.DataFrame(
                {
                    "ticker": [],
                    "country": [],
                    "position_value_EUR": [],
                    "asset_type": [],
                }
            )
        stock_countries = df_latest.filter(pl.col("asset_type") == AssetType.STOCK).select(
            ["ticker", "country", "position_value_EUR", "asset_type"]
        )
        df_country = (
            pl.concat(
                [
                    stock_countries,
                    etf_countries,
                ]
            )
            .with_columns(pl.col("country").replace(COUNTRY_REGION_MAP).alias("region"))
            .group_by("region")
            .agg(pl.col("position_value_EUR").sum())
            .sort("position_value_EUR", descending=True)
            .with_columns(
                (pl.col("position_value_EUR") / pl.col("position_value_EUR").sum() * 100).alias(
                    "relative"
                )
            )
        )
        usa_percentage = (
            df_country.filter(pl.col("region") == "USA").select(pl.col("relative")).item()
        )
        europe_percentage = (
            df_country.filter(pl.col("region") == "Europe").select(pl.col("relative")).item()
        )
        portfolio_data.append(
            {
                "portfolio_name": portfolio.display_name,
                "current": kpis.current_value,
                "current_yoy_dividend": kpis.current_yoy_dividend_value,
                "latest": kpis.latest_date,
                "yoy_return": kpis.yoy_return_pct,
                "usa_percentage": usa_percentage,
                "europe_percentage": europe_percentage,
                "stock_percentage": stock_percentage,
                "real": total_real,
                "stab": total_stab,
                "price": total_price,
                "tech": total_tech,
            }
        )

    df_portfolio = pl.DataFrame(portfolio_data)
    return df_portfolio
