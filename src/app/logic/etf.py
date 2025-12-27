import polars as pl

from src.core.domain_models import AssetType


def calculate_etf_weighted_exposure(
    df_latest: pl.DataFrame,
    df_etf_weights: pl.DataFrame,
) -> pl.DataFrame:
    df_result = (
        df_latest.filter(pl.col("asset_type") == AssetType.ETF)
        .join(
            df_etf_weights,
            on="ticker",
            how="left",
        )
        .with_columns(
            pl.when(pl.col("category").is_null())
            .then(pl.col("weight").fill_null(1.0))
            .otherwise(pl.col("weight"))
            .alias("weight"),
            pl.col("category").fill_null("Unknown").alias("category"),
        )
        .with_columns((pl.col("weight") * pl.col("position_value_EUR")).alias("weighted_value_EUR"))
    )
    return df_result
