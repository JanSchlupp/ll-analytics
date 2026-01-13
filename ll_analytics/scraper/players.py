"""Scrape player profiles and statistics from Learned League."""

import re
from typing import Optional
from bs4 import BeautifulSoup

from .auth import LLSession
from ..config import LL_CATEGORIES


def parse_player_profile(html: str) -> dict:
    """
    Parse a player profile page.

    Args:
        html: Raw HTML of the player profile page

    Returns:
        Dictionary with player data including category stats
    """
    soup = BeautifulSoup(html, "lxml")
    data = {
        "username": None,
        "display_name": None,
        "category_stats": {},
        "overall_pct": None,
    }

    # Extract username from page
    # Structure may vary - this is a template that needs adjustment
    # based on actual LL page structure
    title = soup.find("title")
    if title:
        # Title might be like "LearnedLeague - Player Name"
        title_text = title.get_text()
        if " - " in title_text:
            data["display_name"] = title_text.split(" - ")[-1].strip()

    # Look for category statistics table
    # LL typically shows a table with category percentages
    # This selector needs to be adjusted based on actual HTML structure
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                category_name = cells[0].get_text().strip()
                if category_name in LL_CATEGORIES:
                    # Try to extract percentage
                    pct_text = cells[1].get_text().strip()
                    pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", pct_text)
                    if pct_match:
                        pct = float(pct_match.group(1))
                        # Convert to decimal if it's a percentage
                        if pct > 1:
                            pct = pct / 100
                        data["category_stats"][category_name] = pct

    # Try to find overall percentage
    overall_pattern = re.search(r"Overall[:\s]+(\d+(?:\.\d+)?)\s*%?", html, re.IGNORECASE)
    if overall_pattern:
        pct = float(overall_pattern.group(1))
        data["overall_pct"] = pct / 100 if pct > 1 else pct

    return data


def scrape_player_profile(session: LLSession, username: str) -> Optional[dict]:
    """
    Scrape a player's profile page.

    Args:
        session: Authenticated LLSession
        username: Player's LL username

    Returns:
        Player data dictionary, or None if scraping failed
    """
    html = session.get(f"/profiles.php?{username}")
    if not html:
        return None

    data = parse_player_profile(html)
    data["username"] = username
    return data


def parse_player_match_history(html: str) -> list[dict]:
    """
    Parse a player's match history page.

    Args:
        html: Raw HTML of match history page

    Returns:
        List of match data dictionaries
    """
    soup = BeautifulSoup(html, "lxml")
    matches = []

    # Find match history table
    # Structure needs to be adjusted based on actual LL page
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) >= 4:
                match = {
                    "match_day": None,
                    "opponent": None,
                    "score": None,
                    "opponent_score": None,
                    "tca": None,  # Total correct answers
                    "opponent_tca": None,
                }

                # Parse cells - structure depends on actual LL HTML
                # This is a template
                try:
                    match["match_day"] = int(cells[0].get_text().strip())
                    match["opponent"] = cells[1].get_text().strip()

                    score_text = cells[2].get_text().strip()
                    score_match = re.match(r"(\d+)\s*-\s*(\d+)", score_text)
                    if score_match:
                        match["score"] = int(score_match.group(1))
                        match["opponent_score"] = int(score_match.group(2))

                    matches.append(match)
                except (ValueError, IndexError):
                    continue

    return matches


def parse_rundle_standings(html: str) -> list[dict]:
    """
    Parse a rundle standings page.

    Args:
        html: Raw HTML of rundle standings

    Returns:
        List of player standings in the rundle
    """
    soup = BeautifulSoup(html, "lxml")
    standings = []

    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) >= 3:
                try:
                    entry = {
                        "rank": int(cells[0].get_text().strip()),
                        "username": cells[1].get_text().strip(),
                        "points": None,
                        "tca": None,
                    }

                    # Try to extract points/TCA
                    if len(cells) >= 4:
                        entry["points"] = int(cells[2].get_text().strip())
                        entry["tca"] = int(cells[3].get_text().strip())

                    standings.append(entry)
                except (ValueError, IndexError):
                    continue

    return standings
