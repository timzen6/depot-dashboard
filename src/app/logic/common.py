import polars as pl

from src.core.strategy_engine import StrategyEngine


def get_sorted_occurrences(df: pl.DataFrame, column: str, descending: bool = True) -> list[str]:
    """Get unique occurrences of values in a column, sorted by frequency."""
    return (
        df.select(column)
        .drop_nulls()
        .group_by(column)
        .agg(pl.count().alias("count"))
        .sort("count", descending=descending)
        .select([column])
        .to_series()
        .to_list()
    )


def get_strategy_factor_profiles(
    metadata: pl.DataFrame,
    strategy_engine: StrategyEngine,
) -> pl.DataFrame:
    """Get strategy factor profiles for given positions."""
    df_profiles = (
        strategy_engine.join_factor_profiles(
            metadata,
            sector_column="sector",
            include_zero=True,
            include_sector_reference=True,
        )
        .unpivot(
            index=["ticker", "sector"],
            variable_name="factor",
            value_name="value",
        )
        .with_columns(
            pl.col("factor").str.ends_with("_ref").alias("is_sector_reference"),
            pl.col("factor").str.replace("_ref", "").alias("factor"),
        )
    )
    return df_profiles
