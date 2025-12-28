import polars as pl

from src.core.domain_models import AssetType, Sector

""" Constants and mappings for financial sectors, currencies, and countries.

"""


def get_sector_emoji_from_str(sector_str: str) -> str:
    """Get the emoji representation for a given sector string."""
    # use ghost for unknown sectors
    default_emoji = "👻"
    try:
        sector = Sector(sector_str)
        return SECTOR_EMOJI.get(sector, default_emoji)
    except ValueError:
        return default_emoji


SECTOR_EMOJI = {
    Sector.TECHNOLOGY: "💻",
    Sector.HEALTHCARE: "💊",
    Sector.FINANCIALS: "💰",
    Sector.CONSUMER_DISCRETIONARY: "🛍️",
    Sector.CONSUMER_STAPLES: "🧼",
    Sector.ENERGY: "🛢️",
    Sector.INDUSTRIALS: "🏭",
    Sector.MATERIALS: "🧪️",
    Sector.UTILITIES: "🔌",
    Sector.REAL_ESTATE: "🏠",
    Sector.COMMUNICATION: "📡",
}

CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
}

COUNTRY_FLAGS = {
    "United States": "🇺🇸",
    "Germany": "🇩🇪",
    "France": "🇫🇷",
    "United Kingdom": "🇬🇧",
    "Japan": "🇯🇵",
    "Canada": "🇨🇦",
    "Switzerland": "🇨🇭",
    "Netherlands": "🇳🇱",
    "Italy": "🇮🇹",
    "Spain": "🇪🇸",
    "Sweden": "🇸🇪",
    "Denmark": "🇩🇰",
    "Finland": "🇫🇮",
    "Taiwan": "🇹🇼",
    "South Korea": "🇰🇷",
    # Add more countries as needed
}

# Selected companies get a custom emoji
COMPANY_EMOJIS = {
    "AAPL": "🍎",
    "MSFT": "🪟",
    "AMZN": "📦",
    "ASML.AS": "🔬",
    "AI.PA": "💧",
    "SU.PA": "⚡",
    "MC.PA": "👜",
    "OR.PA": "💄",
    "RMS.PA": "🐎",
    "V": "💳",
    "MA": "💸",
    "SPGI": "📊",
    "NOVO-B.CO": "💉",
    "ATCO-A.ST": "🛠️",
    "LISP.SW": "🍫",
    "ROG.SW": "💊",
    "SY1.DE": "🌸",
    "UNA.AS": "🧴",
    "MUV2.DE": "☂️",
    "EL.PA": "🕶️",
}


def assign_info_emojis(
    df_data: pl.DataFrame,
    sector_col: str = "sector",
    country_col: str = "country",
    asset_col: str = "asset_type",
    name_col: str = "name",
) -> pl.DataFrame:
    """Assign an 'info' column with emojis based on sector and country."""

    if asset_col and asset_col in df_data.columns:
        df_data = df_data.with_columns(
            pl.when(pl.col(asset_col) == AssetType.STOCK)
            .then(
                pl.col(country_col).replace(COUNTRY_FLAGS, default="🏳️")
                + pl.col(sector_col).replace(SECTOR_EMOJI, default="👻")
            )
            .otherwise(
                pl.lit("📑")
                + pl.when(pl.col(name_col).str.to_lowercase().str.contains("europe"))
                .then(pl.lit("🇪🇺"))
                .otherwise(pl.lit("🌍"))
            )
            .alias("info")
        )
    else:
        df_data = df_data.with_columns(
            pl.col(country_col).replace(COUNTRY_FLAGS, default="🏳️")
            + pl.col(sector_col).replace(SECTOR_EMOJI, default="👻").alias("info")
        )
    return df_data.with_columns(
        pl.col("ticker").replace(COMPANY_EMOJIS, default="🏢").alias("ticker_emoji")
    )
