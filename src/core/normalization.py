from src.core.domain_models import Sector

SECTOR_SYNONYMS: dict[str, str] = {
    "information technology": Sector.TECHNOLOGY,
    "tech": Sector.TECHNOLOGY,
    "it": Sector.TECHNOLOGY,
    "health care": Sector.HEALTHCARE,
    "healthcare": Sector.HEALTHCARE,
    "health": Sector.HEALTHCARE,
    "basic materials": Sector.MATERIALS,
    "materials": Sector.MATERIALS,
    "chemicals": Sector.MATERIALS,
    "communication services": Sector.COMMUNICATION,
    "communications": Sector.COMMUNICATION,
    "communication": Sector.COMMUNICATION,
    "telecom": Sector.COMMUNICATION,
    "telecommunications": Sector.COMMUNICATION,
    "telecommunication": Sector.COMMUNICATION,
    "consumer discretionary": Sector.CONSUMER_DISCRETIONARY,
    "discretionary": Sector.CONSUMER_DISCRETIONARY,
    "consumer cyclical": Sector.CONSUMER_DISCRETIONARY,
    "cyclical": Sector.CONSUMER_DISCRETIONARY,
    "consumer staples": Sector.CONSUMER_STAPLES,
    "staples": Sector.CONSUMER_STAPLES,
    "consumer defensive": Sector.CONSUMER_STAPLES,
    "consumer non-cyclical": Sector.CONSUMER_STAPLES,
    "consumer goods": Sector.CONSUMER_STAPLES,
    "finance": Sector.FINANCIALS,
    "financial services": Sector.FINANCIALS,
    "real estate": Sector.REAL_ESTATE,
    "realty": Sector.REAL_ESTATE,
    "reit": Sector.REAL_ESTATE,
}


def sector_normalization(name: str) -> Sector | None:
    """Normalizes sector names to standard Sector enum values."""
    key = name.strip().lower()
    if key in SECTOR_SYNONYMS:
        return Sector(SECTOR_SYNONYMS[key])
    try:
        # also convert to title case
        norm_key = key.replace("_", " ").lower().title()
        return Sector(norm_key)
    except ValueError:
        return None
