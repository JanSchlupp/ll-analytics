"""Scrape match day results from Learned League."""

import re
from typing import Optional
from bs4 import BeautifulSoup

from .auth import LLSession


def parse_match_day_results(html: str) -> dict:
    """
    Parse a match day results page.

    Args:
        html: Raw HTML of match day results

    Returns:
        Dictionary with match day data including all player results
    """
    soup = BeautifulSoup(html, "lxml")
    data = {
        "match_day": None,
        "season": None,
        "matches": [],
        "questions": [],
    }

    # Extract match day number from page
    title = soup.find("title")
    if title:
        day_match = re.search(r"Day\s*(\d+)", title.get_text())
        if day_match:
            data["match_day"] = int(day_match.group(1))

    # Parse questions section
    # LL shows questions with categories and correct percentages
    question_section = soup.find_all("div", class_="question")
    if not question_section:
        # Try alternative selectors
        question_section = soup.find_all(attrs={"data-question": True})

    for i, q_elem in enumerate(question_section, 1):
        question = {
            "number": i,
            "category": None,
            "correct_pct": None,
            "text": None,
        }

        # Try to extract category
        cat_elem = q_elem.find(class_="category")
        if cat_elem:
            question["category"] = cat_elem.get_text().strip()

        # Try to extract correct percentage
        pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", q_elem.get_text())
        if pct_match:
            question["correct_pct"] = float(pct_match.group(1)) / 100

        # Try to extract question text
        text_elem = q_elem.find(class_="text")
        if text_elem:
            question["text"] = text_elem.get_text().strip()

        data["questions"].append(question)

    # Parse individual match results
    match_tables = soup.find_all("table", class_="match")
    if not match_tables:
        match_tables = soup.find_all("table")

    for table in match_tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 5:
                # Structure: Player1 | Score | vs | Score | Player2
                # This needs adjustment based on actual HTML
                try:
                    match = {
                        "player1": cells[0].get_text().strip(),
                        "player1_score": int(cells[1].get_text().strip()),
                        "player2_score": int(cells[3].get_text().strip()),
                        "player2": cells[4].get_text().strip(),
                    }
                    data["matches"].append(match)
                except (ValueError, IndexError):
                    continue

    return data


def parse_player_match_detail(html: str) -> dict:
    """
    Parse detailed results for a single player's match.

    This shows which questions they got right/wrong and defense points.

    Args:
        html: Raw HTML of match detail page

    Returns:
        Dictionary with detailed match results
    """
    soup = BeautifulSoup(html, "lxml")
    data = {
        "player": None,
        "opponent": None,
        "questions": [],  # Which questions correct/incorrect
        "defense_given": [],  # Points assigned to opponent
        "defense_received": [],  # Points opponent assigned to us
    }

    # Parse question results
    # LL typically shows a grid of Q1-Q6 with correct/incorrect markers
    result_cells = soup.find_all(class_="result")
    if not result_cells:
        # Try finding by pattern - correct answers often marked with checkmark or color
        result_cells = soup.find_all(attrs={"data-correct": True})

    for i, cell in enumerate(result_cells[:6], 1):  # Max 6 questions
        correct = None
        cell_text = cell.get_text().strip().lower()
        cell_class = " ".join(cell.get("class", []))

        # Determine if correct - various possible indicators
        if "correct" in cell_class or "right" in cell_class or "✓" in cell_text or "1" in cell_text:
            correct = True
        elif "incorrect" in cell_class or "wrong" in cell_class or "✗" in cell_text or "0" in cell_text:
            correct = False

        if correct is not None:
            data["questions"].append({
                "number": i,
                "correct": correct,
            })

    # Parse defense points
    defense_cells = soup.find_all(class_="defense")
    for i, cell in enumerate(defense_cells[:6], 1):
        try:
            points = int(cell.get_text().strip())
            data["defense_received"].append({
                "question": i,
                "points": points,
            })
        except ValueError:
            continue

    return data


def scrape_match_day(
    session: LLSession,
    season: int,
    match_day: int,
    rundle: Optional[str] = None
) -> Optional[dict]:
    """
    Scrape results for a specific match day.

    Args:
        session: Authenticated LLSession
        season: Season number
        match_day: Match day number (1-25)
        rundle: Optional rundle identifier

    Returns:
        Match day data, or None if scraping failed
    """
    # URL structure needs to be determined from actual LL site
    path = f"/match.php?season={season}&day={match_day}"
    if rundle:
        path += f"&rundle={rundle}"

    html = session.get(path)
    if not html:
        return None

    data = parse_match_day_results(html)
    data["season"] = season
    return data


def scrape_player_answers(
    session: LLSession,
    username: str,
    season: int,
    match_day: int
) -> Optional[dict]:
    """
    Scrape a player's detailed answers for a match day.

    Args:
        session: Authenticated LLSession
        username: Player's username
        season: Season number
        match_day: Match day number

    Returns:
        Detailed match results, or None if scraping failed
    """
    # URL structure needs to be determined
    path = f"/match_detail.php?username={username}&season={season}&day={match_day}"

    html = session.get(path)
    if not html:
        return None

    data = parse_player_match_detail(html)
    data["player"] = username
    data["season"] = season
    data["match_day"] = match_day
    return data
