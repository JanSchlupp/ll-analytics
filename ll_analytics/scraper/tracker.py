"""Scrape player tracker data from Learned League."""

import re
from bs4 import BeautifulSoup

from .auth import LLSession
from ..logging import get_logger

logger = get_logger(__name__)


def parse_tracker(html: str, season: int) -> list[dict]:
    """
    Parse the user's player tracker page.

    Args:
        html: Raw HTML from /tracker/tracker.php
        season: Season number to filter by

    Returns:
        List of {'username': str, 'rundle': str, 'll_id': int | None}
    """
    if not html or len(html) < 1000:
        return []

    soup = BeautifulSoup(html, 'lxml')
    tracked = []

    for link in soup.find_all('a', href=re.compile(r'standings\.php\?\d+&[A-Z]_')):
        href = link.get('href', '')
        match = re.search(r'standings\.php\?(\d+)&([A-Za-z0-9_]+)', href)
        if not match:
            continue

        link_season = int(match.group(1))
        rundle = match.group(2)

        if link_season != season:
            continue

        username = link.get_text(strip=True)

        # Try to find the ll_id from a nearby profile link
        parent = link.find_parent('tr')
        ll_id = None
        if parent:
            profile_link = parent.find('a', href=re.compile(r'profiles\.php\?\d+'))
            if profile_link:
                id_match = re.search(r'profiles\.php\?(\d+)', profile_link.get('href', ''))
                if id_match:
                    ll_id = int(id_match.group(1))

        if username and rundle:
            tracked.append({
                'username': username,
                'rundle': rundle,
                'll_id': ll_id,
            })

    return tracked


def scrape_tracker(session: LLSession, season: int) -> list[dict]:
    """
    Scrape the user's player tracker to get tracked players and their rundles.

    Args:
        session: Authenticated LLSession
        season: Season number to filter by

    Returns:
        List of {'username': str, 'rundle': str, 'll_id': int | None}
    """
    try:
        html = session.get('/tracker/tracker.php')
        return parse_tracker(html, season)
    except Exception as e:
        logger.error("Error scraping tracker: %s", e)
        return []
