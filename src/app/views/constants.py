from src.core.domain_models import Sector

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
