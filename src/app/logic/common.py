import polars as pl


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
