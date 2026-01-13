"""Scrape question data from Learned League."""

import re
from typing import Optional
from bs4 import BeautifulSoup

from .auth import LLSession
from ..config import LL_CATEGORIES


def normalize_category(raw_category: str) -> Optional[str]:
    """
    Normalize a category name to match our standard list.

    Args:
        raw_category: Raw category text from LL

    Returns:
        Normalized category name, or None if no match
    """
    raw_lower = raw_category.lower().strip()

    for category in LL_CATEGORIES:
        if category.lower() == raw_lower:
            return category
        # Handle abbreviations or partial matches
        if raw_lower in category.lower() or category.lower() in raw_lower:
            return category

    # Handle specific known variations
    variations = {
        "am. history": "American History",
        "american hist": "American History",
        "world hist": "World History",
        "bus/econ": "Business/Economics",
        "business": "Business/Economics",
        "food": "Food/Drink",
        "games": "Games/Sport",
        "sport": "Games/Sport",
        "sports": "Games/Sport",
        "pop": "Pop Music",
        "classical": "Classical Music",
        "misc": "Miscellaneous",
        "tv": "Television",
    }

    for key, value in variations.items():
        if key in raw_lower:
            return value

    return None


def parse_match_day_page(html: str) -> dict:
    """
    Parse a match day page from Learned League.

    The page contains questions in div.ind-Q20 elements with format:
    Q#.CATEGORY - Question text

    And a stats table with correct percentages by rundle.

    Args:
        html: Raw HTML from match.php

    Returns:
        Dictionary with questions and stats
    """
    soup = BeautifulSoup(html, "lxml")
    result = {
        "questions": [],
        "rundle_stats": {},  # rundle_name -> [Q1%, Q2%, Q3%, Q4%, Q5%, Q6%]
    }

    # Parse questions from div.ind-Q20 elements
    for div in soup.find_all("div", class_="ind-Q20"):
        text = div.get_text(strip=True)
        # Parse Q#.CATEGORY - Question text
        match = re.match(r"Q(\d+)\.([A-Z\s/]+)\s*-\s*(.+)", text)
        if match:
            q_num = int(match.group(1))
            raw_category = match.group(2).strip()
            q_text = match.group(3)

            question = {
                "number": q_num,
                "category": normalize_category(raw_category) or raw_category,
                "text": q_text,
                "answer": None,
            }
            result["questions"].append(question)

    # Sort questions by number
    result["questions"].sort(key=lambda q: q["number"])

    # Parse answers from div.a-red elements (they follow the questions)
    answers = soup.find_all("div", class_="a-red")
    for i, ans_div in enumerate(answers):
        if i < len(result["questions"]):
            result["questions"][i]["answer"] = ans_div.get_text(strip=True)

    # Parse rundle stats table
    # Table has rows like: Rundle | Forf% | Q1 | Q2 | Q3 | Q4 | Q5 | Q6
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 8:
                rundle_name = cells[0].get_text(strip=True)
                if rundle_name and not rundle_name.startswith("Rundle"):
                    try:
                        # Cells 2-7 are Q1-Q6 percentages
                        q_pcts = []
                        for cell in cells[2:8]:
                            pct_text = cell.get_text(strip=True)
                            pct = float(pct_text) / 100 if pct_text.isdigit() else None
                            q_pcts.append(pct)
                        result["rundle_stats"][rundle_name] = q_pcts
                    except (ValueError, IndexError):
                        continue

    return result


def parse_question_page(html: str) -> list[dict]:
    """
    Parse a page containing questions (legacy format).

    Args:
        html: Raw HTML containing question data

    Returns:
        List of question dictionaries
    """
    # Try new format first
    result = parse_match_day_page(html)
    if result["questions"]:
        return result["questions"]

    # Fall back to generic parsing
    soup = BeautifulSoup(html, "lxml")
    questions = []

    # Try various selectors for question containers
    question_containers = (
        soup.find_all(class_="question") or
        soup.find_all(class_="q-container") or
        soup.find_all("div", attrs={"data-question": True})
    )

    for i, container in enumerate(question_containers, 1):
        question = {
            "number": i,
            "category": None,
            "text": None,
            "answer": None,
            "rundle_correct_pct": None,
            "league_correct_pct": None,
        }

        # Extract category
        cat_elem = (
            container.find(class_="category") or
            container.find(class_="cat") or
            container.find("span", class_="cat")
        )
        if cat_elem:
            raw_cat = cat_elem.get_text().strip()
            question["category"] = normalize_category(raw_cat) or raw_cat

        # Extract question text
        text_elem = (
            container.find(class_="question-text") or
            container.find(class_="text") or
            container.find("p")
        )
        if text_elem:
            question["text"] = text_elem.get_text().strip()

        # Extract answer if available
        answer_elem = (
            container.find(class_="answer") or
            container.find(class_="correct-answer")
        )
        if answer_elem:
            question["answer"] = answer_elem.get_text().strip()

        questions.append(question)

    return questions


def scrape_season_questions(
    session: LLSession,
    season: int,
    match_day: Optional[int] = None
) -> tuple[list[dict], dict]:
    """
    Scrape questions for a season (or specific match day).

    Args:
        session: Authenticated LLSession
        season: Season number
        match_day: Optional specific match day (scrapes all if None)

    Returns:
        Tuple of (list of question dictionaries, dict of rundle stats by day)
    """
    all_questions = []
    all_rundle_stats = {}  # day -> rundle_name -> [pcts]

    if match_day:
        days = [match_day]
    else:
        days = range(1, 26)  # Match days 1-25

    for day in days:
        # LL URL format: /match.php?{season}&{day}
        path = f"/match.php?{season}&{day}"
        html = session.get(path)

        if not html or len(html) < 100:
            continue

        result = parse_match_day_page(html)
        questions = result["questions"]

        for q in questions:
            q["season"] = season
            q["match_day"] = day
            all_questions.append(q)

        if result["rundle_stats"]:
            all_rundle_stats[day] = result["rundle_stats"]

        print(f"  Day {day}: {len(questions)} questions, {len(result['rundle_stats'])} rundles")

    return all_questions, all_rundle_stats


def scrape_question_stats(session: LLSession, season: int) -> dict:
    """
    Scrape aggregate question statistics for a season.

    Args:
        session: Authenticated LLSession
        season: Season number

    Returns:
        Dictionary with category-level statistics
    """
    path = f"/stats.php?season={season}"
    html = session.get(path)

    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    stats = {
        "season": season,
        "categories": {},
        "overall_correct_pct": None,
    }

    # Parse category statistics
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 2:
                cat_text = cells[0].get_text().strip()
                category = normalize_category(cat_text)

                if category:
                    pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%?", cells[1].get_text())
                    if pct_match:
                        stats["categories"][category] = float(pct_match.group(1)) / 100

    return stats
