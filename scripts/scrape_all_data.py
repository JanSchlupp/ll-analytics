"""Scrape all available data for rundle players."""

import sys
import os
import time
import argparse

# Fix encoding for Windows
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ll_analytics.database import get_connection, init_db
from ll_analytics.scraper.auth import LLSession
from ll_analytics.config import Config
from bs4 import BeautifulSoup


def scrape_tracker(session: LLSession, season: int) -> list[dict]:
    """
    Scrape the user's player tracker to get tracked players and their rundles.

    Returns list of {'username': str, 'rundle': str, 'll_id': int}
    """
    import re

    try:
        html = session.get('/tracker/tracker.php')
        if not html or len(html) < 1000:
            return []

        soup = BeautifulSoup(html, 'lxml')
        tracked = []

        # Find all player rows in the tracker table
        # Look for links to standings pages which contain rundle info
        for link in soup.find_all('a', href=re.compile(r'standings\.php\?\d+&[A-Z]_')):
            href = link.get('href', '')
            # Extract season and rundle from URL like /standings.php?107&D_Galaxy_Div_2
            match = re.search(r'standings\.php\?(\d+)&([A-Za-z0-9_]+)', href)
            if match:
                link_season = int(match.group(1))
                rundle = match.group(2)

                # Only get rundles for the target season
                if link_season != season:
                    continue

                # Get player username from link text or nearby cell
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
                        'll_id': ll_id
                    })

        return tracked

    except Exception as e:
        print(f"Error scraping tracker: {e}")
        return []


def scrape_player_ids(session: LLSession, season: int, rundle: str) -> dict[str, int]:
    """
    Scrape player LL IDs from the standings page.

    Returns dict mapping username -> ll_id.
    """
    import re

    html = session.get(f'/standings.php?{season}&{rundle}')
    if not html:
        return {}

    soup = BeautifulSoup(html, 'lxml')
    player_ids = {}

    # Find all profile links
    for link in soup.find_all('a', href=re.compile(r'/profiles\.php\?\d+')):
        href = link.get('href', '')
        match = re.search(r'/profiles\.php\?(\d+)', href)
        if match:
            ll_id = int(match.group(1))
            # Get username from the parent cell's text (link is in same cell as username)
            cell = link.find_parent('td')
            if cell:
                username = cell.get_text(strip=True)
                if username and username not in player_ids:
                    player_ids[username] = ll_id

    return player_ids


def scrape_standings_stats(session: LLSession, season: int, rundle: str) -> list[dict]:
    """
    Scrape aggregate stats from standings_ex.php for all players in a rundle.

    Returns list of player stat dictionaries.
    """
    # First get player IDs from regular standings page
    player_ids = scrape_player_ids(session, season, rundle)

    html = session.get(f'/standings_ex.php?{season}&{rundle}')
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    players = []

    # Find the main standings table
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue

        # Check header row for expected columns
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

        # Expected columns: Rank, '', Player, W, L, T, PTS, MPD, TMP, TCA, PCA, etc.
        if 'Player' not in headers and 'TCA' not in headers:
            continue

        # Map column positions
        col_map = {h: i for i, h in enumerate(headers)}

        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 10:
                continue

            try:
                # Get player username from text in Player column
                player_cell = cells[col_map.get('Player', 2)]
                username = player_cell.get_text(strip=True)
                if not username:
                    continue

                # Parse stats
                rank = cells[col_map.get('Rank', 0)].get_text(strip=True)
                wins = cells[col_map.get('W', 3)].get_text(strip=True)
                losses = cells[col_map.get('L', 4)].get_text(strip=True)
                ties = cells[col_map.get('T', 5)].get_text(strip=True)
                pts = cells[col_map.get('PTS', 6)].get_text(strip=True)
                tca = cells[col_map.get('TCA', 9)].get_text(strip=True)
                pca = cells[col_map.get('PCA', 10)].get_text(strip=True) if 'PCA' in col_map else None

                players.append({
                    'username': username,
                    'll_id': player_ids.get(username),  # Include LL ID
                    'rank': int(rank) if rank.isdigit() else None,
                    'wins': int(wins) if wins.isdigit() else 0,
                    'losses': int(losses) if losses.isdigit() else 0,
                    'ties': int(ties) if ties.isdigit() else 0,
                    'pts': int(pts) if pts.isdigit() else 0,
                    'tca': int(tca) if tca.isdigit() else 0,
                    'pca': float(pca) if pca and pca.replace('.', '').isdigit() else None,
                })
            except Exception as e:
                continue

    return players


def scrape_rundle_matchday(session: LLSession, season: int, match_day: int, rundle: str) -> dict | None:
    """
    Scrape the rundle match day page to get ALL players' per-question answers.

    URL format: /match.php?{season}&{day}&{rundle}

    Returns {
        'questions': [
            {'num': 1, 'category': 'GEOGRAPHY', 'text': '...', 'answer': '...'},
            ...
        ],
        'player_answers': [
            {'ll_id': 12345, 'q1_correct': True, 'q1_defense': 0, ...},
            ...
        ]
    }
    """
    import re

    try:
        html = session.get(f'/match.php?{season}&{match_day}&{rundle}')
        if not html or len(html) < 1000:
            return None

        soup = BeautifulSoup(html, 'lxml')

        result = {
            'questions': [],
            'player_answers': []
        }

        # Parse questions from qacontainer
        qa_container = soup.find('div', class_='qacontainer')
        if qa_container:
            qa_rows = qa_container.find_all('div', class_='qarow')
            for row in qa_rows:
                q_link = row.find('a', href=re.compile(r'question\.php'))
                if q_link:
                    # Extract question number from href like /question.php?107&1&3
                    href = q_link.get('href', '')
                    q_match = re.search(r'question\.php\?\d+&\d+&(\d+)', href)
                    q_num = int(q_match.group(1)) if q_match else 0

                    # Get the text content (category and question)
                    row_text = row.get_text()
                    # Pattern: Q#. CATEGORY - question text
                    cat_match = re.search(r'Q\d+\.\s*([A-Z/\s]+)\s*-\s*(.+)', row_text)
                    if cat_match:
                        category = cat_match.group(1).strip()
                        question_text = cat_match.group(2).strip()
                    else:
                        category = ''
                        question_text = row_text

                    # Get answer from div.a-red
                    answer_div = row.find('div', class_='a-red')
                    answer = answer_div.get_text(strip=True) if answer_div else ''

                    # Clean up question text (remove answer if it was included)
                    if answer and answer in question_text:
                        question_text = question_text.replace(answer, '').strip()

                    result['questions'].append({
                        'num': q_num,
                        'category': category,
                        'text': question_text,
                        'answer': answer
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

                # Get player ll_id from profile link
                player_cell = cells[7]  # Player column
                link = player_cell.find('a', href=re.compile(r'profiles\.php'))
                if not link:
                    continue

                href = link.get('href', '')
                id_match = re.search(r'profiles\.php\?(\d+)', href)
                if not id_match:
                    continue

                ll_id = int(id_match.group(1))

                # Parse Q1-Q6 answers (first 6 columns)
                player_data = {'ll_id': ll_id}
                for q_idx in range(6):
                    cell = cells[q_idx]
                    # Defense points are the number
                    defense = cell.get_text(strip=True)
                    try:
                        defense = int(defense)
                    except:
                        defense = 0

                    # Correct/incorrect from class (c1=correct, c0=incorrect)
                    classes = cell.get('class', [])
                    correct = 'c1' in classes

                    player_data[f'q{q_idx+1}_correct'] = correct
                    player_data[f'q{q_idx+1}_defense'] = defense

                result['player_answers'].append(player_data)

        return result

    except Exception as e:
        print(f"Error scraping match day {match_day}: {e}")
        return None


def scrape_my_answers(session: LLSession, season: int) -> list[dict]:
    """
    Scrape the logged-in user's answers for all match days.
    Uses the pastanswers.php form.

    Returns list of {match_day, question_number, correct, my_answer, correct_answer} dicts.
    """
    results = []

    for day in range(1, 26):
        print(f"  Day {day}...", end=" ", flush=True)

        try:
            response = session.session.post(
                'https://www.learnedleague.com/thorsten/pastanswers.php',
                data={'season': str(season), 'matchday': str(day)}
            )
            html = response.text

            if not html or len(html) < 1000:
                print("no data")
                continue

            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table')

            day_results = []
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # Skip header
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        q_text = cells[0].get_text(strip=True)
                        correct_answer = cells[1].get_text(strip=True)
                        my_answer = cells[2].get_text(strip=True)

                        # Extract question number and text like "1. A heist..."
                        if q_text and q_text[0].isdigit():
                            parts = q_text.split('.', 1)
                            q_num = int(parts[0])
                            # Get full question text (after the number and period)
                            full_question_text = parts[1].strip() if len(parts) > 1 else ""

                            # Determine correctness from the scoring cell image
                            # submitted.png = correct, notsubmitted.png = incorrect
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

            results.extend(day_results[:6])  # Max 6 questions per day
            print(f"{len(day_results)} questions")

        except Exception as e:
            print(f"error: {e}")

        time.sleep(0.5)

    return results


def scrape_match_results(session: LLSession, season: int, rundle: str) -> list[dict]:
    """
    Scrape match results for all days in a rundle.

    Returns list of {match_day, player1, player2, p1_score, p2_score, p1_tca, p2_tca, ll_match_id}
    """
    import re
    results = []

    for day in range(1, 26):
        print(f"  Day {day}...", end=" ", flush=True)

        try:
            html = session.get(f'/match.php?{season}&{day}&{rundle}')
            if not html or 'Not a valid' in html:
                print("no data")
                continue

            soup = BeautifulSoup(html, 'lxml')
            tables = soup.find_all('table')
            day_matches = []

            # First table contains match results
            # Row format: ['', 'Player1', 'Score1(TCA1)  Score2(TCA2)', 'Player2', '']
            for table in tables:
                rows = table.find_all('tr')

                for row in rows:
                    links = row.find_all('a', href=lambda h: h and 'profiles.php' in h)

                    if len(links) == 2:
                        # Player names are in the img alt attribute inside the link
                        p1_img = links[0].find('img')
                        p2_img = links[1].find('img')
                        p1_name = p1_img.get('alt', '') if p1_img else links[0].get_text(strip=True)
                        p2_name = p2_img.get('alt', '') if p2_img else links[1].get_text(strip=True)

                        # Find the match ID link (format: /match.php?id=12345678)
                        ll_match_id = None
                        match_link = row.find('a', href=lambda h: h and 'match.php?id=' in h)
                        if match_link:
                            id_match = re.search(r'id=(\d+)', match_link.get('href', ''))
                            if id_match:
                                ll_match_id = int(id_match.group(1))

                        # Find the middle cell with scores like "4(4)  5(4)"
                        cells = row.find_all(['td', 'th'])
                        for cell in cells:
                            text = cell.get_text(strip=True)
                            # Pattern: "Score1(TCA1)  Score2(TCA2)" e.g., "4(4)  5(4)"
                            match = re.match(r'(\d+)\((\d+)\)\s*(\d+)\((\d+)\)', text.replace('\xa0', ' '))
                            if match:
                                p1_score = int(match.group(1))
                                p1_tca = int(match.group(2))
                                p2_score = int(match.group(3))
                                p2_tca = int(match.group(4))

                                day_matches.append({
                                    'match_day': day,
                                    'player1': p1_name,
                                    'player2': p2_name,
                                    'p1_score': p1_score,
                                    'p1_tca': p1_tca,
                                    'p2_score': p2_score,
                                    'p2_tca': p2_tca,
                                    'll_match_id': ll_match_id,
                                })
                                break

            results.extend(day_matches)
            print(f"{len(day_matches)} matches")

        except Exception as e:
            print(f"error: {e}")

        time.sleep(1.0)  # Rate limit

    return results


def scrape_match_details(session: LLSession, ll_match_id: int) -> dict | None:
    """
    Scrape per-question details for a single match.

    Returns {
        'player1': str, 'player2': str,
        'questions': [
            {'q_num': 1, 'p1_correct': bool, 'p2_correct': bool, 'p1_defense': int, 'p2_defense': int,
             'category': str, 'ca_pct': float},
            ...
        ]
    }
    """
    import re

    try:
        html = session.get(f'/match.php?id={ll_match_id}')
        if not html or 'Not a valid' in html:
            return None

        soup = BeautifulSoup(html, 'lxml')

        # Find the inner table with defense assignments
        inner_table = soup.find('table', class_='tbltop_inner')
        if not inner_table:
            return None

        rows = inner_table.find_all('tr')
        if len(rows) < 3:
            return None

        # Row 0 is header, Row 1 is player1, Row 2 is player2
        p1_row = rows[1].find_all('td')
        p2_row = rows[2].find_all('td')

        if len(p1_row) < 8 or len(p2_row) < 8:
            return None

        # Extract player names (from first cell, format: "PlayerName (W-L-T)")
        p1_name = re.sub(r'\s*\(\d+-\d+-\d+\)', '', p1_row[0].get_text(strip=True))
        p2_name = re.sub(r'\s*\(\d+-\d+-\d+\)', '', p2_row[0].get_text(strip=True))

        # Get the question table with correct/incorrect info
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

            # Cell 0 has category, Cell 1 has CA%
            category = cells[0].get_text(strip=True) if len(cells) > 0 else None
            ca_pct_text = cells[1].get_text(strip=True) if len(cells) > 1 else None
            ca_pct = None
            if ca_pct_text:
                # Parse "45%" to 0.45
                ca_match = re.search(r'(\d+)%', ca_pct_text)
                if ca_match:
                    ca_pct = int(ca_match.group(1)) / 100.0

            # Cell 2 is player1's result, Cell 3 is player2's result
            # Class 'ind-Yes2' = correct, 'ind-No2' = incorrect
            p1_cell = cells[2]
            p2_cell = cells[3]

            p1_correct = 'ind-Yes2' in p1_cell.get('class', [])
            p2_correct = 'ind-Yes2' in p2_cell.get('class', [])

            # Defense values from the inner table (cols 1-6)
            # Note: Defense values shown are what opponent assigned
            # p1_defense = defense player2 assigned to player1's question
            # p2_defense = defense player1 assigned to player2's question
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

    except Exception as e:
        print(f"Error scraping match {ll_match_id}: {e}")
        return None


def scrape_player_profile(session: LLSession, ll_id: int, username: str = None) -> dict | None:
    """
    Scrape a player's profile to get their lifetime category stats.

    Args:
        session: Authenticated LL session
        ll_id: The player's numeric LL ID (used in /profiles.php?{ll_id})
        username: Optional username for logging purposes

    Returns {
        'username': str,
        'categories': [
            {'name': str, 'correct': int, 'total': int, 'pct': float},
            ...
        ]
    }
    """
    import re

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

    try:
        html = session.get(f'/profiles.php?{ll_id}')
        if not html or 'Member not found' in html or 'not an active player' in html:
            return None

        soup = BeautifulSoup(html, 'lxml')
        categories = []

        # Find the category table - look for table with "Category" header and "Career" column
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue

            # Check if this is the category table (has Category header)
            header = rows[0].get_text(strip=True)
            if 'Category' not in header or 'Career' not in header:
                continue

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue

                cat_abbrev = cells[0].get_text(strip=True).upper()
                cat_name = CATEGORY_MAP.get(cat_abbrev)
                if not cat_name:
                    continue

                # Career column is typically cells[1], format is "correct-total"
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

            # Found the category table, no need to check other tables
            break

        if not categories:
            return None

        return {
            'username': username or f'player_{ll_id}',
            'categories': categories,
        }

    except Exception as e:
        print(f"Error scraping profile for {username or ll_id}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Scrape Learned League data')
    parser.add_argument('--season', type=int, default=107, help='Season number')
    parser.add_argument('--rundle', type=str, default='C_Skyline', help='Rundle name')
    args = parser.parse_args()

    target_rundle = args.rundle
    target_season = args.season

    print("Initializing database...")
    init_db()

    print("\nLogging into Learned League...")
    session = LLSession()
    if not session.login(Config.LL_USERNAME, Config.LL_PASSWORD):
        print("Failed to login!")
        return
    print(f"Successfully logged in as {Config.LL_USERNAME}")

    # Get season and rundle info
    with get_connection() as conn:
        season = conn.execute(
            "SELECT * FROM seasons WHERE season_number = ?",
            (target_season,)
        ).fetchone()

        if not season:
            print(f"Season {target_season} not found!")
            return

        season_id = season["id"]
        season_num = season["season_number"]

        # Get target rundle
        rundle = conn.execute(
            "SELECT * FROM rundles WHERE name = ? AND season_id = ?",
            (target_rundle, season_id)
        ).fetchone()

        if not rundle:
            print(f"Rundle {target_rundle} not found! Creating it...")
            # Parse rundle level from name (e.g. C_Skyline -> C)
            level = target_rundle.split('_')[0] if '_' in target_rundle else target_rundle[0]
            conn.execute(
                "INSERT INTO rundles (season_id, league, level, name) VALUES (?, ?, ?, ?)",
                (season_id, 'LL', level, target_rundle)
            )
            conn.commit()
            rundle = conn.execute(
                "SELECT * FROM rundles WHERE name = ? AND season_id = ?",
                (target_rundle, season_id)
            ).fetchone()

        rundle_id = rundle["id"]

    # Part 1: Scrape aggregate standings for all players
    print(f"\n=== Scraping standings for {target_rundle} (Season {season_num}) ===")
    players_stats = scrape_standings_stats(session, season_num, target_rundle)
    print(f"Found {len(players_stats)} players with stats")

    # Update player_rundles with the stats
    with get_connection() as conn:
        for p in players_stats:
            # Get or create player
            player = conn.execute(
                "SELECT id FROM players WHERE ll_username = ?",
                (p['username'],)
            ).fetchone()

            if player:
                player_id = player['id']
                # Update ll_id if we have it and it's not set
                if p.get('ll_id'):
                    conn.execute(
                        "UPDATE players SET ll_id = ? WHERE id = ? AND (ll_id IS NULL OR ll_id != ?)",
                        (p['ll_id'], player_id, p['ll_id'])
                    )
            else:
                cursor = conn.execute(
                    "INSERT INTO players (ll_username, ll_id) VALUES (?, ?)",
                    (p['username'], p.get('ll_id'))
                )
                player_id = cursor.lastrowid

            # Update player_rundles
            conn.execute("""
                INSERT OR REPLACE INTO player_rundles (player_id, rundle_id, final_rank)
                VALUES (?, ?, ?)
            """, (player_id, rundle_id, p['rank']))

            print(f"  {p['username']}: Rank {p['rank']}, TCA {p['tca']}, W-L-T: {p['wins']}-{p['losses']}-{p['ties']}")

        conn.commit()

    # Part 2: Scrape logged-in user's detailed answers
    print(f"\n=== Scraping {Config.LL_USERNAME}'s answers for Season {season_num} ===")
    my_answers = scrape_my_answers(session, season_num)
    print(f"Scraped {len(my_answers)} answers")

    # Get logged-in user's player ID
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?",
            (Config.LL_USERNAME,)
        ).fetchone()

        if not player:
            print(f"Player {Config.LL_USERNAME} not found in database!")
            return

        player_id = player['id']

        # Get question mapping
        questions = conn.execute("""
            SELECT id, match_day, question_number
            FROM questions
            WHERE season_id = ?
        """, (season_id,)).fetchall()

        question_map = {}
        for q in questions:
            key = (q["match_day"], q["question_number"])
            question_map[key] = q["id"]

        # Save answers and update question text
        saved = 0
        questions_updated = 0
        for ans in my_answers:
            q_key = (ans['match_day'], ans['question_number'])
            q_id = question_map.get(q_key)

            if q_id:
                try:
                    # Save the answer
                    conn.execute("""
                        INSERT OR REPLACE INTO answers (player_id, question_id, correct)
                        VALUES (?, ?, ?)
                    """, (player_id, q_id, ans['correct']))
                    saved += 1

                    # Update question text and correct answer if available
                    if ans.get('question_text') or ans.get('correct_answer'):
                        conn.execute("""
                            UPDATE questions
                            SET question_text = COALESCE(?, question_text),
                                correct_answer = COALESCE(?, correct_answer)
                            WHERE id = ?
                        """, (ans.get('question_text'), ans.get('correct_answer'), q_id))
                        questions_updated += 1
                except Exception as e:
                    pass

        conn.commit()
        print(f"Saved {saved} answers for {Config.LL_USERNAME}")
        print(f"Updated {questions_updated} questions with text/answers")

    # Part 3: Scrape match results for all days
    print(f"\n=== Scraping match results for {target_rundle} (Season {season_num}) ===")
    matches = scrape_match_results(session, season_num, target_rundle)
    print(f"Scraped {len(matches)} total match records")

    # Save match results
    with get_connection() as conn:
        # Build player name -> id mapping
        players_db = conn.execute("SELECT id, ll_username FROM players").fetchall()
        player_map = {p['ll_username']: p['id'] for p in players_db}

        saved_matches = 0
        match_ids_to_scrape = []  # (db_match_id, ll_match_id) pairs

        for m in matches:
            p1_id = player_map.get(m['player1'])
            p2_id = player_map.get(m['player2'])

            if p1_id and p2_id:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO matches
                        (season_id, match_day, player1_id, player2_id, player1_score, player2_score, player1_tca, player2_tca, ll_match_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (season_id, m['match_day'], p1_id, p2_id, m['p1_score'], m['p2_score'], m['p1_tca'], m['p2_tca'], m.get('ll_match_id')))
                    saved_matches += 1

                    # Track for per-question scraping
                    if m.get('ll_match_id'):
                        db_match = conn.execute("""
                            SELECT id FROM matches
                            WHERE season_id = ? AND match_day = ? AND player1_id = ? AND player2_id = ?
                        """, (season_id, m['match_day'], p1_id, p2_id)).fetchone()
                        if db_match:
                            match_ids_to_scrape.append((db_match['id'], m['ll_match_id']))
                except Exception as e:
                    pass

        conn.commit()
        print(f"Saved {saved_matches} matches to database")

    # Part 4: Scrape per-question details for all matches
    print(f"\n=== Scraping per-question details for {len(match_ids_to_scrape)} matches ===")
    with get_connection() as conn:
        # Build category name -> id mapping
        categories = conn.execute("SELECT id, name FROM categories").fetchall()
        category_map = {c['name']: c['id'] for c in categories}

        scraped_details = 0
        updated_details = 0
        for i, (db_match_id, ll_match_id) in enumerate(match_ids_to_scrape):
            # Check if already scraped with category data
            existing = conn.execute(
                "SELECT COUNT(*) as c, SUM(CASE WHEN category_id IS NOT NULL THEN 1 ELSE 0 END) as with_cat FROM match_questions WHERE match_id = ?",
                (db_match_id,)
            ).fetchone()

            if existing['c'] > 0 and existing['with_cat'] == existing['c']:
                continue  # Already have complete per-question data

            details = scrape_match_details(session, ll_match_id)
            if details and details['questions']:
                for q in details['questions']:
                    # Get category_id from category name
                    category_id = category_map.get(q.get('category')) if q.get('category') else None

                    conn.execute("""
                        INSERT OR REPLACE INTO match_questions
                        (match_id, question_num, player1_correct, player2_correct, player1_defense, player2_defense, category_id, question_ca_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (db_match_id, q['q_num'], q['p1_correct'], q['p2_correct'], q['p1_defense'], q['p2_defense'], category_id, q.get('ca_pct')))

                if existing['c'] > 0:
                    updated_details += 1
                else:
                    scraped_details += 1

            if (i + 1) % 50 == 0:
                print(f"  Scraped {i + 1}/{len(match_ids_to_scrape)} matches...")
                conn.commit()

            time.sleep(0.5)  # Rate limit

        conn.commit()
        print(f"Scraped per-question details for {scraped_details} new matches")
        print(f"Updated {updated_details} matches with category/CA% data")

    # Part 5: Scrape player profiles for lifetime category stats
    print(f"\n=== Scraping player profiles for lifetime category stats ===")
    with get_connection() as conn:
        # Get all players in the rundle (with ll_id)
        players = conn.execute("""
            SELECT p.id, p.ll_username, p.ll_id
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            WHERE pr.rundle_id = ?
        """, (rundle_id,)).fetchall()

        # Build category name -> id mapping
        categories = conn.execute("SELECT id, name FROM categories").fetchall()
        category_map = {c['name']: c['id'] for c in categories}

        scraped_profiles = 0
        skipped_no_id = 0
        for i, player in enumerate(players):
            # Skip if no ll_id
            if not player['ll_id']:
                skipped_no_id += 1
                continue

            # Check if already have lifetime stats for this player
            existing = conn.execute(
                "SELECT COUNT(*) as c FROM player_lifetime_stats WHERE player_id = ?",
                (player['id'],)
            ).fetchone()['c']

            if existing >= 15:  # Already have most categories
                continue

            profile = scrape_player_profile(session, player['ll_id'], player['ll_username'])
            if profile and profile['categories']:
                for cat in profile['categories']:
                    cat_id = category_map.get(cat['name'])
                    if cat_id:
                        conn.execute("""
                            INSERT OR REPLACE INTO player_lifetime_stats
                            (player_id, category_id, correct_pct, total_questions)
                            VALUES (?, ?, ?, ?)
                        """, (player['id'], cat_id, cat['pct'], cat['total']))

                scraped_profiles += 1
                print(f"  {player['ll_username']}: {len(profile['categories'])} categories")

            if (i + 1) % 10 == 0:
                conn.commit()

            time.sleep(1.0)  # Rate limit - be gentle on profile pages

        conn.commit()
        if skipped_no_id > 0:
            print(f"Skipped {skipped_no_id} players without LL ID")
        print(f"Scraped profiles for {scraped_profiles} players")

    # Part 6: Scrape ALL players' answers from rundle match day pages
    print(f"\n=== Scraping all players' answers from rundle match day pages ===")
    with get_connection() as conn:
        # Build ll_id -> player_id mapping
        players_db = conn.execute("SELECT id, ll_id FROM players WHERE ll_id IS NOT NULL").fetchall()
        ll_id_to_player = {p['ll_id']: p['id'] for p in players_db}

        # Build category name -> id mapping (for question updates)
        categories = conn.execute("SELECT id, name FROM categories").fetchall()
        category_map = {c['name']: c['id'] for c in categories}

        # Also map abbreviated category names
        CATEGORY_ABBREV_MAP = {
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

        total_answers_saved = 0
        total_questions_updated = 0

        # Get players in this rundle
        rundle_players = conn.execute("""
            SELECT p.id, p.ll_id FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            WHERE pr.rundle_id = ?
        """, (rundle_id,)).fetchall()
        rundle_player_ids = {p['id'] for p in rundle_players}

        for day in range(1, 26):
            print(f"  Day {day}...", end=" ", flush=True)

            # Check if we already have answers for this day from players in THIS rundle
            existing_count = conn.execute("""
                SELECT COUNT(DISTINCT a.player_id) as c
                FROM answers a
                JOIN questions q ON a.question_id = q.id
                JOIN player_rundles pr ON a.player_id = pr.player_id
                WHERE q.season_id = ? AND q.match_day = ? AND pr.rundle_id = ?
            """, (season_id, day, rundle_id)).fetchone()['c']

            if existing_count >= len(rundle_player_ids) - 2:  # Already have most players' answers
                print(f"already have {existing_count}/{len(rundle_player_ids)} players' answers, skipping")
                continue

            data = scrape_rundle_matchday(session, season_num, day, target_rundle)
            if not data:
                print("no data")
                continue

            day_answers = 0
            day_questions = 0

            # Update questions with text and answers
            for q in data.get('questions', []):
                q_num = q['num']
                cat_abbrev = q.get('category', '').strip()
                cat_name = CATEGORY_ABBREV_MAP.get(cat_abbrev, cat_abbrev)
                cat_id = category_map.get(cat_name)

                # Update the question record
                conn.execute("""
                    UPDATE questions
                    SET question_text = COALESCE(?, question_text),
                        correct_answer = COALESCE(?, correct_answer),
                        category_id = COALESCE(?, category_id)
                    WHERE season_id = ? AND match_day = ? AND question_number = ?
                """, (q.get('text'), q.get('answer'), cat_id, season_id, day, q_num))
                day_questions += 1

            # Get question IDs for this day
            questions = conn.execute("""
                SELECT id, question_number FROM questions
                WHERE season_id = ? AND match_day = ?
            """, (season_id, day)).fetchall()
            q_num_to_id = {q['question_number']: q['id'] for q in questions}

            # Save player answers
            for pa in data.get('player_answers', []):
                ll_id = pa.get('ll_id')
                player_id = ll_id_to_player.get(ll_id)

                if not player_id:
                    continue  # Player not in our database

                for q_num in range(1, 7):
                    q_id = q_num_to_id.get(q_num)
                    if not q_id:
                        continue

                    correct = pa.get(f'q{q_num}_correct', False)
                    defense = pa.get(f'q{q_num}_defense', 0)

                    try:
                        conn.execute("""
                            INSERT OR REPLACE INTO answers
                            (player_id, question_id, correct, defense_points_assigned)
                            VALUES (?, ?, ?, ?)
                        """, (player_id, q_id, correct, defense))
                        day_answers += 1
                    except Exception as e:
                        pass

            conn.commit()
            total_answers_saved += day_answers
            total_questions_updated += day_questions
            print(f"{day_answers} answers, {day_questions} questions updated")

            time.sleep(1.0)  # Rate limit

        print(f"Total: {total_answers_saved} answers saved, {total_questions_updated} questions updated")

    # Summary
    print("\n=== Summary ===")
    with get_connection() as conn:
        total_players = conn.execute("""
            SELECT COUNT(*) as c FROM player_rundles WHERE rundle_id = ?
        """, (rundle_id,)).fetchone()['c']

        total_answers = conn.execute("""
            SELECT COUNT(*) as c FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE q.season_id = ?
        """, (season_id,)).fetchone()['c']

        total_matches = conn.execute("""
            SELECT COUNT(*) as c FROM matches WHERE season_id = ?
        """, (season_id,)).fetchone()['c']

        total_match_questions = conn.execute("""
            SELECT COUNT(*) as c FROM match_questions mq
            JOIN matches m ON mq.match_id = m.id
            WHERE m.season_id = ?
        """, (season_id,)).fetchone()['c']

        players_with_lifetime = conn.execute("""
            SELECT COUNT(DISTINCT player_id) as c FROM player_lifetime_stats
        """).fetchone()['c']

        print(f"Players in {target_rundle}: {total_players}")
        print(f"Total answers in database: {total_answers}")
        print(f"Total matches in database: {total_matches}")
        print(f"Total match questions: {total_match_questions}")
        print(f"Players with lifetime stats: {players_with_lifetime}")

    print("\nDone!")


if __name__ == "__main__":
    main()
