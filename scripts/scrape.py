#!/usr/bin/env python3
"""
Scrape data from Learned League.

Usage:
    python scripts/scrape.py --season 99
    python scripts/scrape.py --season 99 --rundle "Pacific A"
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from ll_analytics.database import init_db
from ll_analytics.scraper import LLScraper
from ll_analytics.config import Config


def main():
    parser = argparse.ArgumentParser(description="Scrape Learned League data")
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="Season number to scrape"
    )
    parser.add_argument(
        "--rundle",
        type=str,
        default=None,
        help="Optional: limit to specific rundle"
    )
    parser.add_argument(
        "--questions-only",
        action="store_true",
        help="Only scrape questions, not match results"
    )
    parser.add_argument(
        "--matches-only",
        action="store_true",
        help="Only scrape match results, not questions"
    )
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Skip detailed player answer scraping"
    )

    args = parser.parse_args()

    # Validate config
    errors = Config.validate()
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        print("\nPlease set up your .env file with LL credentials.")
        sys.exit(1)

    # Initialize database
    print("Ensuring database is initialized...")
    init_db()

    # Create scraper and log in
    scraper = LLScraper()

    print("\nLogging in to Learned League...")
    if not scraper.login():
        print("Failed to log in. Check your credentials.")
        sys.exit(1)

    try:
        # Determine what to scrape
        include_questions = not args.matches_only
        include_matches = not args.questions_only
        include_details = not args.skip_details

        # Run the scrape
        summary = scraper.scrape_season(
            args.season,
            include_questions=include_questions,
            include_matches=include_matches,
            include_player_details=include_details,
            rundle_filter=args.rundle,
        )

        print("\nScraping summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

    finally:
        scraper.logout()


if __name__ == "__main__":
    main()
