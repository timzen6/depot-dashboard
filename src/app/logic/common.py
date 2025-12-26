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


COUNTRY_REGION_MAP = {
    # The use is its own group as it is often a large portion alone
    "United States": "USA",
    "Canada": "North America",
    "Mexico": "North America",
    "Germany": "Europe",
    "France": "Europe",
    "United Kingdom": "Europe",
    "Italy": "Europe",
    "Spain": "Europe",
    "China": "Asia Pacific Emerging",
    "Japan": "Asia Pacific Developed",
    "India": "Asia Pacific Emerging",
    "Australia": "Asia Pacific Developed",
    "Brazil": "South America",
    "Netherlands": "Europe",
    "Switzerland": "Europe",
    "Sweden": "Europe",
    "Belgium": "Europe",
    "South Korea": "Asia Pacific Developed",
    "Taiwan": "Asia Pacific Developed",
    "Russia": "Europe",
    "Ireland": "Europe",
    "Singapore": "Asia Pacific Developed",
    "Hong Kong": "Asia Pacific Developed",
    "Norway": "Europe",
    "Denmark": "Europe",
    "Finland": "Europe",
    "Austria": "Europe",
    "Portugal": "Europe",
    "New Zealand": "Asia Pacific Developed",
    "South Africa": "Africa",
    "Saudi Arabia": "Asia Pacific Emerging",
    "United Arab Emirates": "Asia Pacific Emerging",
    "Thailand": "Asia Pacific Emerging",
    "Malaysia": "Asia Pacific Emerging",
    "Indonesia": "Asia Pacific Emerging",
    "Philippines": "Asia Pacific Emerging",
    "Poland": "Europe",
    "Hungary": "Europe",
    "Greece": "Europe",
    "Qatar": "Asia Pacific Emerging",
    "Kuwait": "Asia Pacific Emerging",
    "Israel": "Asia Pacific Emerging",
}
