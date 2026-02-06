"""Scrape match day results from Learned League."""

import re
import time
from typing import Optional
from bs4 import BeautifulSoup

from .auth import LLSession
from .players import CATEGORY_MAP
from ..logging import get_logger

logger = get_logger(__name__)


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
    question_section = soup.find_all("div", class_="question")
    if not question_section:
        question_section = soup.find_all(attrs={"data-question": True})

    for i, q_elem in enumerate(question_section, 1):
        question = {
            "number": i,
            "category": None,
            "correct_pct": None,
            "text": None,
        }

        cat_elem = q_elem.find(class_="category")
        if cat_elem:
            question["category"] = cat_elem.get_text().strip()

        pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", q_elem.get_text())
        if pct_match:
            question["correct_pct"] = float(pct_match.group(1)) / 100

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

    Args:
        html: Raw HTML of match detail page

    Returns:
        Dictionary with detailed match results
    """
    soup = BeautifulSoup(html, "lxml")
    data = {
        "player": None,
        "opponent": None,
        "questions": [],
        "defense_given": [],
        "defense_received": [],
    }

    result_cells = soup.find_all(class_="result")
    if not result_cells:
        result_cells = soup.find_all(attrs={"data-correct": True})

    for i, cell in enumerate(result_cells[:6], 1):
        correct = None
        cell_text = cell.get_text().strip().lower()
        cell_class = " ".join(cell.get("class", []))

        if "correct" in cell_class or "right" in cell_class or "\u2713" in cell_text or "1" in cell_text:
            correct = True
        elif "incorrect" in cell_class or "wrong" in cell_class or "\u2717" in cell_text or "0" in cell_text:
            correct = False

        if correct is not None:
            data["questions"].append({
                "number": i,
                "correct": correct,
            })

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


def parse_match_detail_page(html: str) -> dict | None:
    """
    Parse per-question details for a single match (from /match.php?id=...).

    Uses the actual LL HTML structure: tbltop_inner table for defense,
    QTable for correct/incorrect indicators.

    Args:
        html: Raw HTML from /match.php?id={match_id}

    Returns:
        Dict with 'player1', 'player2', 'questions' or None if parsing failed.
    """
    if not html or 'Not a valid' in html:
        return None

    soup = BeautifulSoup(html, 'lxml')

    inner_table = soup.find('table', class_='tbltop_inner')
    if not inner_table:
        return None

    rows = inner_table.find_all('tr')
    if len(rows) < 3:
        return None

    p1_row = rows[1].find_all('td')
    p2_row = rows[2].find_all('td')

    if len(p1_row) < 8 or len(p2_row) < 8:
        return None

    p1_name = re.sub(r'\s*\(\d+-\d+-\d+\)', '', p1_row[0].get_text(strip=True))
    p2_name = re.sub(r'\s*\(\d+-\d+-\d+\)', '', p2_row[0].get_text(strip=True))

    q_table = soup.find('table', class_='QTable')
    if not q_table:
        return None

    q_rows = q_table.find_all('tr')

    questions = []
    for q_num in range(1, 7):
        if q_num >= len(q_rows):
            break

        q_row = q_rows[q_num]
        cells = q_row.find_all('td')

        if len(cells) < 4:
            continue

        category = cells[0].get_text(strip=True) if len(cells) > 0 else None
        ca_pct_text = cells[1].get_text(strip=True) if len(cells) > 1 else None
        ca_pct = None
        if ca_pct_text:
            ca_match = re.search(r'(\d+)%', ca_pct_text)
            if ca_match:
                ca_pct = int(ca_match.group(1)) / 100.0

        p1_cell = cells[2]
        p2_cell = cells[3]

        p1_correct = 'ind-Yes2' in p1_cell.get('class', [])
        p2_correct = 'ind-Yes2' in p2_cell.get('class', [])

        p1_defense = int(p1_row[q_num].get_text(strip=True)) if p1_row[q_num].get_text(strip=True).isdigit() else 0
        p2_defense = int(p2_row[q_num].get_text(strip=True)) if p2_row[q_num].get_text(strip=True).isdigit() else 0

        questions.append({
            'q_num': q_num,
            'p1_correct': p1_correct,
            'p2_correct': p2_correct,
            'p1_defense': p1_defense,
            'p2_defense': p2_defense,
            'category': category,
            'ca_pct': ca_pct,
        })

    return {
        'player1': p1_name,
        'player2': p2_name,
        'questions': questions,
    }


def scrape_match_details(session: LLSession, ll_match_id: int) -> dict | None:
    """
    Scrape per-question details for a single match by LL match ID.

    Args:
        session: Authenticated LLSession
        ll_match_id: LL's internal match ID

    Returns:
        Match detail dict, or None if failed
    """
    try:
        html = session.get(f'/match.php?id={ll_match_id}')
        return parse_match_detail_page(html)
    except Exception as e:
        logger.error("Error scraping match %d: %s", ll_match_id, e)
        return None


def parse_rundle_matchday(html: str) -> dict | None:
    """
    Parse a rundle match day page to extract ALL players' per-question answers.

    Args:
        html: Raw HTML from /match.php?{season}&{day}&{rundle}

    Returns:
        Dict with 'questions' and 'player_answers', or None if parsing failed.
    """
    if not html or len(html) < 1000:
        return None

    soup = BeautifulSoup(html, 'lxml')

    result = {
        'questions': [],
        'player_answers': [],
    }

    # Parse questions from qacontainer
    qa_container = soup.find('div', class_='qacontainer')
    if qa_container:
        qa_rows = qa_container.find_all('div', class_='qarow')
        for row in qa_rows:
            q_link = row.find('a', href=re.compile(r'question\.php'))
            if q_link:
                href = q_link.get('href', '')
                q_match = re.search(r'question\.php\?\d+&\d+&(\d+)', href)
                q_num = int(q_match.group(1)) if q_match else 0

                row_text = row.get_text()
                cat_match = re.search(r'Q\d+\.\s*([A-Z/\s]+)\s*-\s*(.+)', row_text)
                if cat_match:
                    category = cat_match.group(1).strip()
                    question_text = cat_match.group(2).strip()
                else:
                    category = ''
                    question_text = row_text

                answer_div = row.find('div', class_='a-red')
                answer = answer_div.get_text(strip=True) if answer_div else ''

                if answer and answer in question_text:
                    question_text = question_text.replace(answer, '').strip()

                result['questions'].append({
                    'num': q_num,
                    'category': category,
                    'text': question_text,
                    'answer': answer,
                })

    # Parse player answers from the answers table (table index 1)
    tables = soup.find_all('table')
    if len(tables) >= 2:
        answers_table = tables[1]
        rows = answers_table.find_all('tr')[1:]  # Skip header

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 8:
                continue

            player_cell = cells[7]
            link = player_cell.find('a', href=re.compile(r'profiles\.php'))
            if not link:
                continue

            href = link.get('href', '')
            id_match = re.search(r'profiles\.php\?(\d+)', href)
            if not id_match:
                continue

            ll_id = int(id_match.group(1))

            player_data = {'ll_id': ll_id}
            for q_idx in range(6):
                cell = cells[q_idx]
                defense = cell.get_text(strip=True)
                try:
                    defense = int(defense)
                except (ValueError, TypeError):
                    defense = 0

                classes = cell.get('class', [])
                correct = 'c1' in classes

                player_data[f'q{q_idx+1}_correct'] = correct
                player_data[f'q{q_idx+1}_defense'] = defense

            result['player_answers'].append(player_data)

    return result


def scrape_rundle_matchday(
    session: LLSession, season: int, match_day: int, rundle: str
) -> dict | None:
    """
    Scrape the rundle match day page to get ALL players' per-question answers.

    Args:
        session: Authenticated LLSession
        season: Season number
        match_day: Match day number (1-25)
        rundle: Rundle name

    Returns:
        Dict with 'questions' and 'player_answers', or None if failed
    """
    try:
        html = session.get(f'/match.php?{season}&{match_day}&{rundle}')
        return parse_rundle_matchday(html)
    except Exception as e:
        logger.error("Error scraping match day %d: %s", match_day, e)
        return None


def scrape_match_results(
    session: LLSession, season: int, rundle: str
) -> list[dict]:
    """
    Scrape match results for all days in a rundle.

    Args:
        session: Authenticated LLSession
        season: Season number
        rundle: Rundle name

    Returns:
        List of match result dicts with keys:
        match_day, player1, player2, p1_score, p2_score, p1_tca, p2_tca, ll_match_id
    """
    results = []

    for day in range(1, 26):
        logger.info("  Day %d...", day)

        try:
            html = session.get(f'/match.php?{season}&{day}&{rundle}')
            if not html or 'Not a valid' in html:
                logger.debug("  Day %d: no data", day)
                continue

            soup = BeautifulSoup(html, 'lxml')
            day_matches = []

            for table in soup.find_all('table'):
                rows = table.find_all('tr')

                for row in rows:
                    links = row.find_all('a', href=lambda h: h and 'profiles.php' in h)

                    if len(links) == 2:
                        p1_img = links[0].find('img')
                        p2_img = links[1].find('img')
                        p1_name = p1_img.get('alt', '') if p1_img else links[0].get_text(strip=True)
                        p2_name = p2_img.get('alt', '') if p2_img else links[1].get_text(strip=True)

                        ll_match_id = None
                        match_link = row.find('a', href=lambda h: h and 'match.php?id=' in h)
                        if match_link:
                            id_match = re.search(r'id=(\d+)', match_link.get('href', ''))
                            if id_match:
                                ll_match_id = int(id_match.group(1))

                        cells = row.find_all(['td', 'th'])
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            match = re.match(r'(\d+)\((\d+)\)\s*(\d+)\((\d+)\)', text.replace('\xa0', ' '))
                            if match:
                                day_matches.append({
                                    'match_day': day,
                                    'player1': p1_name,
                                    'player2': p2_name,
                                    'p1_score': int(match.group(1)),
                                    'p1_tca': int(match.group(2)),
                                    'p2_score': int(match.group(3)),
                                    'p2_tca': int(match.group(4)),
                                    'll_match_id': ll_match_id,
                                })
                                break

            results.extend(day_matches)
            logger.info("  Day %d: %d matches", day, len(day_matches))

        except Exception as e:
            logger.error("  Day %d: error: %s", day, e)

        time.sleep(1.0)

    return results


def scrape_my_answers(session: LLSession, season: int) -> list[dict]:
    """
    Scrape the logged-in user's answers for all match days.

    Args:
        session: Authenticated LLSession
        season: Season number

    Returns:
        List of answer dicts with keys:
        match_day, question_number, correct, my_answer, correct_answer, question_text
    """
    results = []

    for day in range(1, 26):
        logger.info("  Day %d...", day)

        try:
            response = session.session.post(
                'https://www.learnedleague.com/thorsten/pastanswers.php',
                data={'season': str(season), 'matchday': str(day)}
            )
            html = response.text

            if not html or len(html) < 1000:
                logger.debug("  Day %d: no data", day)
                continue

            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table')

            day_results = []
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        q_text = cells[0].get_text(strip=True)
                        correct_answer = cells[1].get_text(strip=True)
                        my_answer = cells[2].get_text(strip=True)

                        if q_text and q_text[0].isdigit():
                            parts = q_text.split('.', 1)
                            q_num = int(parts[0])
                            full_question_text = parts[1].strip() if len(parts) > 1 else ""

                            is_correct = False
                            if len(cells) >= 4:
                                scoring_cell = cells[3]
                                img = scoring_cell.find('img')
                                if img:
                                    img_src = img.get('src', '')
                                    is_correct = 'submitted.png' in img_src and 'notsubmitted' not in img_src

                            day_results.append({
                                'match_day': day,
                                'question_number': q_num,
                                'correct': is_correct,
                                'my_answer': my_answer,
                                'correct_answer': correct_answer,
                                'question_text': full_question_text,
                            })

            results.extend(day_results[:6])
            logger.info("  Day %d: %d questions", day, len(day_results))

        except Exception as e:
            logger.error("  Day %d: error: %s", day, e)

        time.sleep(0.5)

    return results


def scrape_match_day(
    session: LLSession,
    season: int,
    match_day: int,
    rundle: Optional[str] = None
) -> Optional[dict]:
    """
    Scrape results for a specific match day (generic format).

    Args:
        session: Authenticated LLSession
        season: Season number
        match_day: Match day number (1-25)
        rundle: Optional rundle identifier

    Returns:
        Match day data, or None if scraping failed
    """
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
    path = f"/match_detail.php?username={username}&season={season}&day={match_day}"

    html = session.get(path)
    if not html:
        return None

    data = parse_player_match_detail(html)
    data["player"] = username
    data["season"] = season
    data["match_day"] = match_day
    return data
