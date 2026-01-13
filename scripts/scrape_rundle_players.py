"""Scrape match data for all players in a rundle."""

import sys
import os
import time
import re

# Fix encoding for Windows
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ll_analytics.database import get_connection, init_db
from ll_analytics.scraper.auth import LLSession
from ll_analytics.config import Config
from bs4 import BeautifulSoup


def scrape_player_match_data(session: LLSession, username: str, season: int) -> list[dict]:
    """
    Scrape a player's match results for a season.

    Returns list of {match_day, question_number, correct} dicts.
    """
    results = []

    for day in range(1, 26):
        # Get the player's match page for this day
        path = f"/match.php?{season}&{day}&{username}"
        html = session.get(path)

        if not html or len(html) < 500:
            continue

        soup = BeautifulSoup(html, "lxml")

        # Find the player's results - look for ind-Yes2 (correct) and ind-No2 (incorrect)
        # These are in table cells
        day_results = []

        # Find all question result indicators
        yes_cells = soup.find_all("td", class_="ind-Yes2")
        no_cells = soup.find_all("td", class_="ind-No2")

        # Also try spans
        yes_spans = soup.find_all("span", class_="ind-Yes2")
        no_spans = soup.find_all("span", class_="ind-No2")

        # Also try divs
        yes_divs = soup.find_all("div", class_="ind-Yes2")
        no_divs = soup.find_all("div", class_="ind-No2")

        # Combine all indicators
        correct_count = len(yes_cells) + len(yes_spans) + len(yes_divs)
        incorrect_count = len(no_cells) + len(no_spans) + len(no_divs)

        # If we found indicators, record them
        if correct_count > 0 or incorrect_count > 0:
            # We don't know which specific questions, so record aggregates
            for q in range(1, 7):
                # We'll need to parse more carefully to get per-question results
                pass

        # Alternative: Look for the results table with Q1-Q6 headers
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                # Look for rows with the player's username
                row_text = row.get_text()
                if username.lower() in row_text.lower():
                    # This row might contain the player's results
                    # Parse the cells for correct/incorrect indicators
                    for i, cell in enumerate(cells):
                        cell_classes = cell.get("class", [])
                        if "ind-Yes2" in cell_classes or cell.find(class_="ind-Yes2"):
                            day_results.append({"question_number": i, "correct": True})
                        elif "ind-No2" in cell_classes or cell.find(class_="ind-No2"):
                            day_results.append({"question_number": i, "correct": False})

        # If we still don't have results, try parsing the match page differently
        if not day_results:
            # Look for a pattern like "3-3" or similar score indicators
            # And check individual question cells
            for td in soup.find_all("td"):
                classes = td.get("class", [])
                if "ind-Yes2" in classes:
                    day_results.append({"correct": True})
                elif "ind-No2" in classes:
                    day_results.append({"correct": False})

        # Add match day to results
        for i, r in enumerate(day_results[:6]):  # Max 6 questions per day
            results.append({
                "match_day": day,
                "question_number": i + 1,
                "correct": r.get("correct", False)
            })

        # Small delay between requests
        time.sleep(0.5)

    return results


def main():
    print("Initializing database...")
    init_db()

    print("Logging into Learned League...")
    session = LLSession()
    if not session.login(Config.LL_USERNAME, Config.LL_PASSWORD):
        print("Failed to login!")
        return
    print(f"Successfully logged in as {Config.LL_USERNAME}")

    # Get season and rundle info
    with get_connection() as conn:
        season = conn.execute(
            "SELECT * FROM seasons ORDER BY season_number DESC LIMIT 1"
        ).fetchone()

        if not season:
            print("No season found!")
            return

        season_id = season["id"]
        season_num = season["season_number"]

        # Get C_Skyline rundle
        rundle = conn.execute(
            "SELECT * FROM rundles WHERE name = 'C_Skyline' AND season_id = ?",
            (season_id,)
        ).fetchone()

        if not rundle:
            print("C_Skyline rundle not found!")
            return

        rundle_id = rundle["id"]

        # Get all players in rundle
        players = conn.execute("""
            SELECT p.id, p.ll_username
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            WHERE pr.rundle_id = ?
        """, (rundle_id,)).fetchall()

        players_list = [dict(p) for p in players]

    print(f"\nScraping match data for {len(players_list)} players in Season {season_num}...")
    print("This will take several minutes due to rate limiting.\n")

    # Get question mapping (we need question IDs from database)
    with get_connection() as conn:
        questions = conn.execute("""
            SELECT id, match_day, question_number
            FROM questions
            WHERE season_id = ?
        """, (season_id,)).fetchall()

        question_map = {}
        for q in questions:
            key = (q["match_day"], q["question_number"])
            question_map[key] = q["id"]

    # Scrape each player
    for i, player in enumerate(players_list):
        username = player["ll_username"]
        player_id = player["id"]

        # Check if we already have data for this player
        with get_connection() as conn:
            existing = conn.execute("""
                SELECT COUNT(*) as c FROM answers a
                JOIN questions q ON a.question_id = q.id
                WHERE a.player_id = ? AND q.season_id = ?
            """, (player_id, season_id)).fetchone()["c"]

        if existing >= 150:
            print(f"[{i+1}/{len(players_list)}] {username}: Already have {existing} answers, skipping")
            continue

        print(f"[{i+1}/{len(players_list)}] Scraping {username}...", end=" ", flush=True)

        try:
            results = scrape_player_match_data(session, username, season_num)

            if results:
                # Save to database
                with get_connection() as conn:
                    saved = 0
                    for r in results:
                        q_key = (r["match_day"], r["question_number"])
                        q_id = question_map.get(q_key)

                        if q_id:
                            try:
                                conn.execute("""
                                    INSERT OR REPLACE INTO answers (player_id, question_id, correct)
                                    VALUES (?, ?, ?)
                                """, (player_id, q_id, r["correct"]))
                                saved += 1
                            except Exception as e:
                                pass

                    conn.commit()
                    print(f"saved {saved} answers")
            else:
                print("no data found")

        except Exception as e:
            print(f"error: {e}")

        # Rate limit
        time.sleep(1.5)

    print("\nDone!")

    # Show summary
    with get_connection() as conn:
        total_answers = conn.execute("""
            SELECT COUNT(*) as c FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE q.season_id = ?
        """, (season_id,)).fetchone()["c"]

        players_with_data = conn.execute("""
            SELECT COUNT(DISTINCT a.player_id) as c FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE q.season_id = ?
        """, (season_id,)).fetchone()["c"]

        print(f"\nSummary:")
        print(f"  Total answers in database: {total_answers}")
        print(f"  Players with data: {players_with_data}")


if __name__ == "__main__":
    main()
