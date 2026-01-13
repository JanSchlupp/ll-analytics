"""Main scraper orchestration for Learned League data collection."""

import sqlite3
from typing import Optional
from datetime import datetime

from .auth import LLSession
from .players import scrape_player_profile, parse_rundle_standings
from .matches import scrape_match_day, scrape_player_answers
from .questions import scrape_season_questions

from ..database import (
    get_connection,
    get_or_create_player,
    get_or_create_season,
    get_category_id,
)


class LLScraper:
    """
    Main scraper class that orchestrates data collection from Learned League.

    Usage:
        scraper = LLScraper()
        scraper.login()
        scraper.scrape_season(99)
    """

    def __init__(self):
        self.session = LLSession()

    def login(self, username: Optional[str] = None, password: Optional[str] = None) -> bool:
        """Log in to Learned League."""
        return self.session.login(username, password)

    def logout(self) -> None:
        """Log out."""
        self.session.logout()

    def scrape_season(
        self,
        season_number: int,
        include_questions: bool = True,
        include_matches: bool = True,
        include_player_details: bool = True,
        rundle_filter: Optional[str] = None,
    ) -> dict:
        """
        Scrape all data for a season.

        Args:
            season_number: The LL season number to scrape
            include_questions: Whether to scrape question data
            include_matches: Whether to scrape match results
            include_player_details: Whether to scrape detailed player answers
            rundle_filter: Optional rundle to limit scraping to

        Returns:
            Summary of scraped data
        """
        print(f"\n{'='*50}")
        print(f"Scraping Season {season_number}")
        print(f"{'='*50}")

        summary = {
            "season": season_number,
            "questions_scraped": 0,
            "matches_scraped": 0,
            "players_scraped": 0,
            "answers_scraped": 0,
            "started_at": datetime.now().isoformat(),
        }

        with get_connection() as conn:
            season_id = get_or_create_season(conn, season_number)

            # Scrape questions
            if include_questions:
                print("\n[1/3] Scraping questions...")
                questions = scrape_season_questions(self.session, season_number)
                self._save_questions(conn, season_id, questions)
                summary["questions_scraped"] = len(questions)
                print(f"  Saved {len(questions)} questions")

            # Scrape match results
            if include_matches:
                print("\n[2/3] Scraping match results...")
                for match_day in range(1, 26):
                    match_data = scrape_match_day(
                        self.session, season_number, match_day, rundle_filter
                    )
                    if match_data and match_data.get("matches"):
                        self._save_match_day(conn, season_id, match_data)
                        summary["matches_scraped"] += len(match_data["matches"])
                        print(f"  Day {match_day}: {len(match_data['matches'])} matches")

            # Scrape detailed player answers
            if include_player_details and include_matches:
                print("\n[3/3] Scraping player answer details...")
                # Get all players we've seen in matches
                players = conn.execute(
                    """
                    SELECT DISTINCT p.id, p.ll_username
                    FROM players p
                    JOIN matches m ON p.id = m.player1_id OR p.id = m.player2_id
                    WHERE m.season_id = ?
                    """,
                    (season_id,)
                ).fetchall()

                for player in players:
                    for match_day in range(1, 26):
                        answers = scrape_player_answers(
                            self.session,
                            player["ll_username"],
                            season_number,
                            match_day
                        )
                        if answers and answers.get("questions"):
                            self._save_player_answers(conn, season_id, player["id"], match_day, answers)
                            summary["answers_scraped"] += len(answers["questions"])

                    summary["players_scraped"] += 1
                    if summary["players_scraped"] % 10 == 0:
                        print(f"  Processed {summary['players_scraped']} players...")

            conn.commit()

        summary["finished_at"] = datetime.now().isoformat()
        print(f"\n{'='*50}")
        print(f"Scraping complete!")
        print(f"  Questions: {summary['questions_scraped']}")
        print(f"  Matches: {summary['matches_scraped']}")
        print(f"  Players: {summary['players_scraped']}")
        print(f"  Answers: {summary['answers_scraped']}")
        print(f"{'='*50}\n")

        return summary

    def _save_questions(
        self,
        conn: sqlite3.Connection,
        season_id: int,
        questions: list[dict]
    ) -> None:
        """Save scraped questions to database."""
        for q in questions:
            category_id = get_category_id(conn, q.get("category", "Miscellaneous"))
            if not category_id:
                # Try to find closest match or use Miscellaneous
                category_id = get_category_id(conn, "Miscellaneous")

            conn.execute(
                """
                INSERT OR REPLACE INTO questions
                (season_id, match_day, question_number, category_id, rundle_correct_pct, league_correct_pct, question_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    season_id,
                    q.get("match_day"),
                    q.get("number"),
                    category_id,
                    q.get("rundle_correct_pct"),
                    q.get("league_correct_pct"),
                    q.get("text"),
                )
            )

    def _save_match_day(
        self,
        conn: sqlite3.Connection,
        season_id: int,
        match_data: dict
    ) -> None:
        """Save match day results to database."""
        match_day = match_data.get("match_day")

        for match in match_data.get("matches", []):
            player1_id = get_or_create_player(conn, match["player1"])
            player2_id = get_or_create_player(conn, match["player2"])

            conn.execute(
                """
                INSERT OR REPLACE INTO matches
                (season_id, match_day, player1_id, player2_id, player1_score, player2_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    season_id,
                    match_day,
                    player1_id,
                    player2_id,
                    match.get("player1_score"),
                    match.get("player2_score"),
                )
            )

    def _save_player_answers(
        self,
        conn: sqlite3.Connection,
        season_id: int,
        player_id: int,
        match_day: int,
        answers: dict
    ) -> None:
        """Save player's detailed answers to database."""
        for q in answers.get("questions", []):
            # Find the question ID
            question = conn.execute(
                """
                SELECT id FROM questions
                WHERE season_id = ? AND match_day = ? AND question_number = ?
                """,
                (season_id, match_day, q.get("number"))
            ).fetchone()

            if question:
                # Get defense points if available
                defense_points = None
                for d in answers.get("defense_received", []):
                    if d.get("question") == q.get("number"):
                        defense_points = d.get("points")
                        break

                conn.execute(
                    """
                    INSERT OR REPLACE INTO answers
                    (player_id, question_id, correct, defense_points_assigned)
                    VALUES (?, ?, ?, ?)
                    """,
                    (player_id, question["id"], q.get("correct"), defense_points)
                )

    def scrape_player(self, username: str) -> Optional[dict]:
        """
        Scrape a single player's profile.

        Args:
            username: Player's LL username

        Returns:
            Player data, or None if scraping failed
        """
        return scrape_player_profile(self.session, username)

    def update_player_category_stats(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int,
        category_stats: dict
    ) -> None:
        """Update player's category statistics."""
        for category_name, pct in category_stats.items():
            category_id = get_category_id(conn, category_name)
            if category_id:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO player_category_stats
                    (player_id, category_id, season_id, correct_pct)
                    VALUES (?, ?, ?, ?)
                    """,
                    (player_id, category_id, season_id, pct)
                )
