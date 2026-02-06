"""Scrape player profiles and statistics from Learned League."""

import re
from typing import Optional
from bs4 import BeautifulSoup

from .auth import LLSession
from ..config import LL_CATEGORIES
from ..logging import get_logger

logger = get_logger(__name__)

# Mapping from LL's abbreviated category names to our standard names
CATEGORY_MAP = {
    'AMER HIST': 'American History',
    'ART': 'Art',
    'BUS/ECON': 'Business/Economics',
    'CLASS MUSIC': 'Classical Music',
    'FILM': 'Film',
    'FOOD/DRINK': 'Food/Drink',
    'GAMES/SPORT': 'Games/Sport',
    'GEOGRAPHY': 'Geography',
    'LANGUAGE': 'Language',
    'LIFESTYLE': 'Lifestyle',
    'LITERATURE': 'Literature',
    'MATH': 'Math',
    'POP MUSIC': 'Pop Music',
    'SCIENCE': 'Science',
    'TELEVISION': 'Television',
    'THEATRE': 'Theatre',
    'WORLD HIST': 'World History',
    'MISC': 'Miscellaneous',
}


def parse_player_profile(html: str) -> dict:
    """
    Parse a player profile page (generic format).

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
    title = soup.find("title")
    if title:
        title_text = title.get_text()
        if " - " in title_text:
            data["display_name"] = title_text.split(" - ")[-1].strip()

    # Look for category statistics table
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                category_name = cells[0].get_text().strip()
                if category_name in LL_CATEGORIES:
                    pct_text = cells[1].get_text().strip()
                    pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", pct_text)
                    if pct_match:
                        pct = float(pct_match.group(1))
                        if pct > 1:
                            pct = pct / 100
                        data["category_stats"][category_name] = pct

    # Try to find overall percentage
    overall_pattern = re.search(r"Overall[:\s]+(\d+(?:\.\d+)?)\s*%?", html, re.IGNORECASE)
    if overall_pattern:
        pct = float(overall_pattern.group(1))
        data["overall_pct"] = pct / 100 if pct > 1 else pct

    return data


def parse_player_profile_by_id(html: str) -> dict | None:
    """
    Parse a player's profile page to extract lifetime category stats.

    Uses the actual LL profile page structure with abbreviated category names
    and "correct-total" format in the Career column.

    Args:
        html: Raw HTML from /profiles.php?{ll_id}

    Returns:
        Dict with 'categories' list, or None if parsing failed.
        Each category: {'name': str, 'correct': int, 'total': int, 'pct': float}
    """
    if not html or 'Member not found' in html or 'not an active player' in html:
        return None

    soup = BeautifulSoup(html, 'lxml')
    categories = []

    # Find the category table (has "Category" and "Career" headers)
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue

        header = rows[0].get_text(strip=True)
        if 'Category' not in header or 'Career' not in header:
            continue

        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:
                continue

            cat_abbrev = cells[0].get_text(strip=True).upper()
            cat_name = CATEGORY_MAP.get(cat_abbrev)
            if not cat_name:
                continue

            career_text = cells[1].get_text(strip=True)
            match = re.match(r'(\d+)-(\d+)', career_text)
            if not match:
                continue

            correct = int(match.group(1))
            total = int(match.group(2))
            pct = correct / total if total > 0 else 0

            categories.append({
                'name': cat_name,
                'correct': correct,
                'total': total,
                'pct': pct,
            })

        break  # Found the category table

    return {'categories': categories} if categories else None


def scrape_player_profile(session: LLSession, username: str) -> Optional[dict]:
    """
    Scrape a player's profile page by username.

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


def scrape_player_profile_by_id(
    session: LLSession, ll_id: int, username: str | None = None
) -> dict | None:
    """
    Scrape a player's profile by their numeric LL ID.

    Args:
        session: Authenticated LLSession
        ll_id: The player's numeric LL ID
        username: Optional username for the returned data

    Returns:
        Dict with 'username' and 'categories', or None if failed
    """
    try:
        html = session.get(f'/profiles.php?{ll_id}')
        result = parse_player_profile_by_id(html)
        if result:
            result['username'] = username or f'player_{ll_id}'
        return result
    except Exception as e:
        logger.error("Error scraping profile for %s: %s", username or ll_id, e)
        return None


def scrape_player_ids(session: LLSession, season: int, rundle: str) -> dict[str, int]:
    """
    Scrape player LL IDs from a standings page.

    Args:
        session: Authenticated LLSession
        season: Season number
        rundle: Rundle name

    Returns:
        Dict mapping username -> ll_id
    """
    html = session.get(f'/standings.php?{season}&{rundle}')
    if not html:
        return {}

    soup = BeautifulSoup(html, 'lxml')
    player_ids = {}

    for link in soup.find_all('a', href=re.compile(r'/profiles\.php\?\d+')):
        href = link.get('href', '')
        match = re.search(r'/profiles\.php\?(\d+)', href)
        if match:
            ll_id = int(match.group(1))
            cell = link.find_parent('td')
            if cell:
                username = cell.get_text(strip=True)
                if username and username not in player_ids:
                    player_ids[username] = ll_id

    return player_ids


def scrape_standings_stats(
    session: LLSession, season: int, rundle: str
) -> list[dict]:
    """
    Scrape aggregate stats from standings_ex.php for all players in a rundle.

    Args:
        session: Authenticated LLSession
        season: Season number
        rundle: Rundle name

    Returns:
        List of player stat dictionaries with keys:
        username, ll_id, rank, wins, losses, ties, pts, tca, pca
    """
    # Get player IDs from regular standings page
    player_ids = scrape_player_ids(session, season, rundle)

    html = session.get(f'/standings_ex.php?{season}&{rundle}')
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    players = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue

        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

        if 'Player' not in headers and 'TCA' not in headers:
            continue

        col_map = {h: i for i, h in enumerate(headers)}

        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 10:
                continue

            try:
                player_cell = cells[col_map.get('Player', 2)]
                username = player_cell.get_text(strip=True)
                if not username:
                    continue

                rank = cells[col_map.get('Rank', 0)].get_text(strip=True)
                wins = cells[col_map.get('W', 3)].get_text(strip=True)
                losses = cells[col_map.get('L', 4)].get_text(strip=True)
                ties = cells[col_map.get('T', 5)].get_text(strip=True)
                pts = cells[col_map.get('PTS', 6)].get_text(strip=True)
                tca = cells[col_map.get('TCA', 9)].get_text(strip=True)
                pca = cells[col_map.get('PCA', 10)].get_text(strip=True) if 'PCA' in col_map else None

                players.append({
                    'username': username,
                    'll_id': player_ids.get(username),
                    'rank': int(rank) if rank.isdigit() else None,
                    'wins': int(wins) if wins.isdigit() else 0,
                    'losses': int(losses) if losses.isdigit() else 0,
                    'ties': int(ties) if ties.isdigit() else 0,
                    'pts': int(pts) if pts.isdigit() else 0,
                    'tca': int(tca) if tca.isdigit() else 0,
                    'pca': float(pca) if pca and pca.replace('.', '').isdigit() else None,
                })
            except Exception:
                continue

    return players


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
                    "tca": None,
                    "opponent_tca": None,
                }

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

                    if len(cells) >= 4:
                        entry["points"] = int(cells[2].get_text().strip())
                        entry["tca"] = int(cells[3].get_text().strip())

                    standings.append(entry)
                except (ValueError, IndexError):
                    continue

    return standings
