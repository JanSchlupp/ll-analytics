"""Configuration management for LL Analytics."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the project root directory (where this config.py lives, go up one level)
PROJECT_ROOT = Path(__file__).parent.parent


class Config:
    """Application configuration."""

    # Learned League credentials
    LL_USERNAME: str = os.getenv("LL_USERNAME", "")
    LL_PASSWORD: str = os.getenv("LL_PASSWORD", "")
    LL_BASE_URL: str = "https://learnedleague.com"

    # Database - use absolute path relative to project root
    DATABASE_PATH: Path = Path(os.getenv("DATABASE_PATH", str(PROJECT_ROOT / "data" / "ll_analytics.db")))

    # API settings
    API_HOST: str = os.getenv("API_HOST", "127.0.0.1")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str | None = os.getenv("LOG_FILE")

    # Scraper settings
    REQUEST_DELAY: float = 1.5  # Seconds between requests
    REQUEST_TIMEOUT: int = 30  # Seconds

    # Game defaults
    DEFAULT_SEASON: int = int(os.getenv("DEFAULT_SEASON", "107"))
    DEFAULT_RUNDLE: str = os.getenv("DEFAULT_RUNDLE", "C_Skyline")

    # Metric tunables
    LEVERAGE_START_DAY: int = 12  # Surprise distribution: leverage split after this day
    EARLY_DAYS: range = range(1, 11)   # Late-spike metric: days 1-10
    LATE_DAYS: range = range(20, 26)   # Late-spike metric: days 20-25

    @classmethod
    def ensure_data_dir(cls) -> None:
        """Create data directory if it doesn't exist."""
        cls.DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate(cls) -> list[str]:
        """Validate configuration. Returns list of errors."""
        errors = []
        if not cls.LL_USERNAME:
            errors.append("LL_USERNAME not set in environment")
        if not cls.LL_PASSWORD:
            errors.append("LL_PASSWORD not set in environment")
        return errors


# The 18 Learned League categories
LL_CATEGORIES = [
    "American History",
    "Art",
    "Business/Economics",
    "Classical Music",
    "Film",
    "Food/Drink",
    "Games/Sport",
    "Geography",
    "Language",
    "Lifestyle",
    "Literature",
    "Math",
    "Pop Music",
    "Science",
    "Television",
    "Theatre",
    "World History",
    "Miscellaneous",
]
