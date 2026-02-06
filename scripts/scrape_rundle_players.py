"""Scrape match data for all players in a rundle.

Thin CLI wrapper around LLScraper.scrape_full() with only
standings + rundle answers enabled.

Usage:
    python scripts/scrape_rundle_players.py
    python scripts/scrape_rundle_players.py --season 107 --rundle C_Skyline
"""

import sys
import os
import argparse

# Fix encoding for Windows
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ll_analytics.config import Config
from ll_analytics.database import init_db
from ll_analytics.logging import setup_logging, get_logger
from ll_analytics.scraper import LLScraper

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Scrape match data for all players in a rundle')
    parser.add_argument('--season', type=int, default=Config.DEFAULT_SEASON, help='Season number')
    parser.add_argument('--rundle', type=str, default=Config.DEFAULT_RUNDLE, help='Rundle name')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    args = parser.parse_args()

    setup_logging(args.log_level)

    errors = Config.validate()
    if errors:
        for e in errors:
            logger.error("Config: %s", e)
        sys.exit(1)

    init_db()

    scraper = LLScraper()
    if not scraper.login():
        logger.error("Failed to log in. Check your credentials.")
        sys.exit(1)

    try:
        # Only standings (to ensure players exist) + rundle answers
        result = scraper.scrape_full(
            args.season,
            args.rundle,
            include_standings=True,
            include_my_answers=False,
            include_match_results=False,
            include_match_details=False,
            include_profiles=False,
            include_rundle_answers=True,
        )

        summary = result.to_dict()
        logger.info("Scrape complete: %s", summary['counts'])
        if summary['errors']:
            logger.warning("%d errors occurred", len(summary['errors']))
    finally:
        scraper.logout()


if __name__ == "__main__":
    main()
