#!/usr/bin/env python3
"""
Scrape data from Learned League (question/match-level scraper).

Usage:
    python scripts/scrape.py --season 99
    python scripts/scrape.py --season 99 --rundle C_Skyline
    python scripts/scrape.py --season 99 --questions-only
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ll_analytics.config import Config
from ll_analytics.database import init_db
from ll_analytics.logging import setup_logging, get_logger
from ll_analytics.scraper import LLScraper

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Scrape Learned League data")
    parser.add_argument("--season", type=int, default=Config.DEFAULT_SEASON, help="Season number to scrape")
    parser.add_argument("--rundle", type=str, default=None, help="Optional: limit to specific rundle")
    parser.add_argument("--questions-only", action="store_true", help="Only scrape questions")
    parser.add_argument("--matches-only", action="store_true", help="Only scrape match results")
    parser.add_argument("--skip-details", action="store_true", help="Skip detailed player answer scraping")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)

    errors = Config.validate()
    if errors:
        for e in errors:
            logger.error("Config: %s", e)
        sys.exit(1)

    init_db()

    scraper = LLScraper()
    logger.info("Logging in to Learned League...")
    if not scraper.login():
        logger.error("Failed to log in. Check your credentials.")
        sys.exit(1)

    try:
        summary = scraper.scrape_season(
            args.season,
            include_questions=not args.matches_only,
            include_matches=not args.questions_only,
            include_player_details=not args.skip_details,
            rundle_filter=args.rundle,
        )

        logger.info("Scraping summary:")
        for key, value in summary.items():
            logger.info("  %s: %s", key, value)
    finally:
        scraper.logout()


if __name__ == "__main__":
    main()
