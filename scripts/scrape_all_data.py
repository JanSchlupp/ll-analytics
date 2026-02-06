"""Scrape all available data for a rundle.

Thin CLI wrapper around LLScraper.scrape_full().

Usage:
    python scripts/scrape_all_data.py --season 107 --rundle C_Skyline
    python scripts/scrape_all_data.py --season 107 --rundle C_Skyline --skip-profiles
"""

import argparse
import sys
import os

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
    parser = argparse.ArgumentParser(description='Scrape all Learned League data for a rundle')
    parser.add_argument('--season', type=int, default=Config.DEFAULT_SEASON, help='Season number')
    parser.add_argument('--rundle', type=str, default=Config.DEFAULT_RUNDLE, help='Rundle name')
    parser.add_argument('--skip-standings', action='store_true')
    parser.add_argument('--skip-my-answers', action='store_true')
    parser.add_argument('--skip-match-results', action='store_true')
    parser.add_argument('--skip-match-details', action='store_true')
    parser.add_argument('--skip-profiles', action='store_true')
    parser.add_argument('--skip-rundle-answers', action='store_true')
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
        result = scraper.scrape_full(
            args.season,
            args.rundle,
            include_standings=not args.skip_standings,
            include_my_answers=not args.skip_my_answers,
            include_match_results=not args.skip_match_results,
            include_match_details=not args.skip_match_details,
            include_profiles=not args.skip_profiles,
            include_rundle_answers=not args.skip_rundle_answers,
        )

        summary = result.to_dict()
        logger.info("Scrape complete: %s", summary['counts'])
        if summary['errors']:
            logger.warning("%d errors occurred", len(summary['errors']))
    finally:
        scraper.logout()


if __name__ == "__main__":
    main()
