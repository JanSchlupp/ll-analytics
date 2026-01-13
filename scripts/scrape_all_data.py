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


def scrape_standings_stats(session: LLSession, season: int, rundle: str) -> list[dict]:
    """
    Scrape aggregate stats from standings_ex.php for all players in a rundle.

    Returns list of player stat dictionaries.
    """
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
            {'q_num': 1, 'p1_correct': bool, 'p2_correct': bool, 'p1_defense': int, 'p2_defense': int},
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
            })

        return {
            'player1': p1_name,
            'player2': p2_name,
            'questions': questions,
        }

    except Exception as e:
        print(f"Error scraping match {ll_match_id}: {e}")
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
            else:
                cursor = conn.execute(
                    "INSERT INTO players (ll_username) VALUES (?)",
                    (p['username'],)
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
        scraped_details = 0
        for i, (db_match_id, ll_match_id) in enumerate(match_ids_to_scrape):
            # Check if already scraped
            existing = conn.execute(
                "SELECT COUNT(*) as c FROM match_questions WHERE match_id = ?",
                (db_match_id,)
            ).fetchone()['c']

            if existing > 0:
                continue  # Already have per-question data

            details = scrape_match_details(session, ll_match_id)
            if details and details['questions']:
                for q in details['questions']:
                    conn.execute("""
                        INSERT OR REPLACE INTO match_questions
                        (match_id, question_num, player1_correct, player2_correct, player1_defense, player2_defense)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (db_match_id, q['q_num'], q['p1_correct'], q['p2_correct'], q['p1_defense'], q['p2_defense']))
                scraped_details += 1

            if (i + 1) % 50 == 0:
                print(f"  Scraped {i + 1}/{len(match_ids_to_scrape)} matches...")
                conn.commit()

            time.sleep(0.5)  # Rate limit

        conn.commit()
        print(f"Scraped per-question details for {scraped_details} new matches")

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

        print(f"Players in {target_rundle}: {total_players}")
        print(f"Total answers in database: {total_answers}")
        print(f"Total matches in database: {total_matches}")

    print("\nDone!")


if __name__ == "__main__":
    main()
