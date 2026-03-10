from pathlib import Path

from dataclasses import dataclass


@dataclass(frozen=True)
class League:
    code: str
    name: str
    country: str


BASE_URL = "https://www.football-data.co.uk/mmz4281"
SOURCE_NAME = "football_data_co_uk"
ENTITY_NAME = "matches_odds"
DEFAULT_BUCKET = "football"
BRONZE_PREFIX = "bronze"
LOCAL_RAW_ROOT = Path("data/raw")

DEFAULT_LEAGUES = (
    League("E0", "Premier League", "England"),
    League("SP1", "La Liga", "Spain"),
    League("I1", "Serie A", "Italy"),
    League("D1", "Bundesliga", "Germany"),
    League("F1", "Ligue 1", "France"),
    League("P1", "Primeira Liga", "Portugal"),
)
