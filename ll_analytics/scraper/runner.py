"""Main scraper orchestration for Learned League data collection."""

import sqlite3
import time
from typing import Optional
from datetime import datetime

from .auth import LLSession
from .players import (
    scrape_player_profile,
    scrape_player_profile_by_id,
    scrape_standings_stats,
    scrape_player_ids,
    parse_rundle_standings,
    CATEGORY_MAP,
)
from .matches import (
    scrape_match_day,
    scrape_match_results,
    scrape_match_details,
    scrape_my_answers,
    scrape_rundle_matchday,
    scrape_player_answers,
)
from .questions import scrape_season_questions
from .tracker import scrape_tracker

from ..config import Config
from ..database import (
    get_connection,
    get_or_create_player,
    get_or_create_season,
    get_category_id,
)
from ..logging import get_logger

logger = get_logger(__name__)


class ScrapeResult:
    """Tracks results and errors from a scrape run."""

    def __init__(self):
        self.started_at = datetime.now().isoformat()
        self.finished_at: str | None = None
        self.counts: dict[str, int] = {}
        self.errors: list[dict] = []

    def count(self, key: str, n: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + n

    def error(self, stage: str, detail: str) -> None:
        self.errors.append({"stage": stage, "detail": detail})
        logger.error("[%s] %s", stage, detail)

    def finish(self) -> None:
        self.finished_at = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "counts": self.counts,
            "errors": self.errors,
            "error_count": len(self.errors),
        }


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

    # ── Full pipeline (mirrors scrape_all_data.py) ───────────────────

    def scrape_full(
        self,
        season_number: int,
        rundle: str,
        *,
        include_standings: bool = True,
        include_my_answers: bool = True,
        include_match_results: bool = True,
        include_match_details: bool = True,
        include_profiles: bool = True,
        include_rundle_answers: bool = True,
    ) -> ScrapeResult:
        """
        Run the full 6-part scrape pipeline for a rundle.

        This covers everything scrape_all_data.py does:
          1. Standings + stats
          2. User's own answers
          3. Match results
          4. Per-question match details
          5. Player profiles (lifetime stats)
          6. Rundle match day answers (all players)

        Args:
            season_number: Season number
            rundle: Rundle name (e.g. "C_Skyline")

        Returns:
            ScrapeResult with counts and errors
        """
        result = ScrapeResult()

        logger.info("=" * 50)
        logger.info("Full scrape: Season %d, Rundle %s", season_number, rundle)
        logger.info("=" * 50)

        with get_connection() as conn:
            season_id = get_or_create_season(conn, season_number)
            rundle_id = self._ensure_rundle(conn, season_id, rundle)
            conn.commit()

        if include_standings:
            self._scrape_standings(season_number, rundle, rundle_id, result)

        if include_my_answers:
            self._scrape_my_answers(season_number, result)

        if include_match_results:
            self._scrape_match_results(season_number, rundle, result)

        if include_match_details:
            self._scrape_match_details(season_number, result)

        if include_profiles:
            self._scrape_profiles(rundle_id, result)

        if include_rundle_answers:
            self._scrape_rundle_answers(season_number, rundle, rundle_id, result)

        result.finish()

        logger.info("=" * 50)
        logger.info("Full scrape complete!")
        for key, count in result.counts.items():
            logger.info("  %s: %d", key, count)
        if result.errors:
            logger.warning("  Errors: %d", len(result.errors))
        logger.info("=" * 50)

        return result

    def _ensure_rundle(
        self, conn: sqlite3.Connection, season_id: int, rundle: str
    ) -> int:
        """Get or create a rundle, returning its ID."""
        row = conn.execute(
            "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
            (rundle, season_id),
        ).fetchone()

        if row:
            return row["id"]

        level = rundle.split('_')[0] if '_' in rundle else rundle[0]
        conn.execute(
            "INSERT INTO rundles (season_id, league, level, name) VALUES (?, ?, ?, ?)",
            (season_id, 'LL', level, rundle),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
            (rundle, season_id),
        ).fetchone()
        return row["id"]

    # ── Part 1: Standings ──────────────────────────────────────────

    def _scrape_standings(
        self, season: int, rundle: str, rundle_id: int, result: ScrapeResult
    ) -> None:
        logger.info("[1/6] Scraping standings for %s...", rundle)
        players_stats = scrape_standings_stats(self.session, season, rundle)
        logger.info("  Found %d players with stats", len(players_stats))

        with get_connection() as conn:
            for p in players_stats:
                player = conn.execute(
                    "SELECT id FROM players WHERE ll_username = ?",
                    (p['username'],),
                ).fetchone()

                if player:
                    player_id = player['id']
                    if p.get('ll_id'):
                        conn.execute(
                            "UPDATE players SET ll_id = ? WHERE id = ? AND (ll_id IS NULL OR ll_id != ?)",
                            (p['ll_id'], player_id, p['ll_id']),
                        )
                else:
                    cursor = conn.execute(
                        "INSERT INTO players (ll_username, ll_id) VALUES (?, ?)",
                        (p['username'], p.get('ll_id')),
                    )
                    player_id = cursor.lastrowid

                conn.execute(
                    "INSERT OR REPLACE INTO player_rundles (player_id, rundle_id, final_rank) VALUES (?, ?, ?)",
                    (player_id, rundle_id, p['rank']),
                )

            conn.commit()

        result.count("players_standings", len(players_stats))

    # ── Part 2: My answers ─────────────────────────────────────────

    def _scrape_my_answers(self, season: int, result: ScrapeResult) -> None:
        logger.info("[2/6] Scraping %s's answers...", Config.LL_USERNAME)
        my_answers = scrape_my_answers(self.session, season)
        logger.info("  Scraped %d answers", len(my_answers))

        with get_connection() as conn:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if not season_row:
                result.error("my_answers", f"Season {season} not found")
                return
            season_id = season_row["id"]

            player = conn.execute(
                "SELECT id FROM players WHERE ll_username = ?",
                (Config.LL_USERNAME,),
            ).fetchone()
            if not player:
                result.error("my_answers", f"Player {Config.LL_USERNAME} not found in database")
                return

            question_map = self._get_question_map(conn, season_id)

            saved = 0
            for ans in my_answers:
                q_id = question_map.get((ans['match_day'], ans['question_number']))
                if not q_id:
                    continue
                try:
                    conn.execute(
                        "INSERT OR REPLACE INTO answers (player_id, question_id, correct) VALUES (?, ?, ?)",
                        (player['id'], q_id, ans['correct']),
                    )
                    if ans.get('question_text') or ans.get('correct_answer'):
                        conn.execute(
                            "UPDATE questions SET question_text = COALESCE(?, question_text), correct_answer = COALESCE(?, correct_answer) WHERE id = ?",
                            (ans.get('question_text'), ans.get('correct_answer'), q_id),
                        )
                    saved += 1
                except Exception as e:
                    result.error("my_answers", str(e))

            conn.commit()

        result.count("my_answers", saved)

    # ── Part 3: Match results ──────────────────────────────────────

    def _scrape_match_results(
        self, season: int, rundle: str, result: ScrapeResult
    ) -> None:
        logger.info("[3/6] Scraping match results for %s...", rundle)
        matches = scrape_match_results(self.session, season, rundle)
        logger.info("  Scraped %d match records", len(matches))

        with get_connection() as conn:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if not season_row:
                result.error("match_results", f"Season {season} not found")
                return
            season_id = season_row["id"]

            player_map = {
                p['ll_username']: p['id']
                for p in conn.execute("SELECT id, ll_username FROM players").fetchall()
            }

            saved = 0
            for m in matches:
                p1_id = player_map.get(m['player1'])
                p2_id = player_map.get(m['player2'])
                # Auto-create players we haven't seen before
                if not p1_id:
                    p1_id = get_or_create_player(conn, m['player1'])
                    player_map[m['player1']] = p1_id
                if not p2_id:
                    p2_id = get_or_create_player(conn, m['player2'])
                    player_map[m['player2']] = p2_id
                try:
                    conn.execute("""
                        INSERT INTO matches
                        (season_id, match_day, player1_id, player2_id, player1_score, player2_score, player1_tca, player2_tca, ll_match_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(season_id, match_day, player1_id, player2_id) DO UPDATE SET
                            player1_score = excluded.player1_score,
                            player2_score = excluded.player2_score,
                            player1_tca = excluded.player1_tca,
                            player2_tca = excluded.player2_tca,
                            ll_match_id = excluded.ll_match_id
                    """, (season_id, m['match_day'], p1_id, p2_id, m['p1_score'], m['p2_score'], m['p1_tca'], m['p2_tca'], m.get('ll_match_id')))
                    saved += 1
                except Exception as e:
                    result.error("match_results", str(e))

            conn.commit()

        result.count("matches", saved)

    # ── Part 4: Per-question match details ─────────────────────────

    def _scrape_match_details(self, season: int, result: ScrapeResult) -> None:
        logger.info("[4/6] Scraping per-question match details...")

        with get_connection() as conn:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if not season_row:
                return
            season_id = season_row["id"]

            # Get matches that need detail scraping
            to_scrape = conn.execute("""
                SELECT m.id as db_id, m.ll_match_id
                FROM matches m
                WHERE m.season_id = ? AND m.ll_match_id IS NOT NULL
            """, (season_id,)).fetchall()

            categories = {c['name']: c['id'] for c in conn.execute("SELECT id, name FROM categories").fetchall()}

            scraped = 0
            for i, row in enumerate(to_scrape):
                # Check if already complete
                existing = conn.execute(
                    "SELECT COUNT(*) as c, SUM(CASE WHEN category_id IS NOT NULL THEN 1 ELSE 0 END) as with_cat FROM match_questions WHERE match_id = ?",
                    (row['db_id'],),
                ).fetchone()

                if existing['c'] > 0 and existing['with_cat'] == existing['c']:
                    continue

                details = scrape_match_details(self.session, row['ll_match_id'])
                if details and details['questions']:
                    for q in details['questions']:
                        category_id = categories.get(q.get('category')) if q.get('category') else None
                        conn.execute("""
                            INSERT OR REPLACE INTO match_questions
                            (match_id, question_num, player1_correct, player2_correct, player1_defense, player2_defense, category_id, question_ca_pct)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (row['db_id'], q['q_num'], q['p1_correct'], q['p2_correct'], q['p1_defense'], q['p2_defense'], category_id, q.get('ca_pct')))
                    scraped += 1

                if (i + 1) % 50 == 0:
                    logger.info("  Scraped %d/%d matches...", i + 1, len(to_scrape))
                    conn.commit()

                time.sleep(0.5)

            conn.commit()

        result.count("match_details", scraped)

    # ── Part 5: Player profiles (lifetime stats) ───────────────────

    def _scrape_profiles(self, rundle_id: int, result: ScrapeResult) -> None:
        logger.info("[5/6] Scraping player profiles...")

        with get_connection() as conn:
            players = conn.execute("""
                SELECT p.id, p.ll_username, p.ll_id
                FROM players p
                JOIN player_rundles pr ON p.id = pr.player_id
                WHERE pr.rundle_id = ?
            """, (rundle_id,)).fetchall()

            categories = {c['name']: c['id'] for c in conn.execute("SELECT id, name FROM categories").fetchall()}

            scraped = 0
            skipped_no_id = 0
            for i, player in enumerate(players):
                if not player['ll_id']:
                    skipped_no_id += 1
                    continue

                existing = conn.execute(
                    "SELECT COUNT(*) as c FROM player_lifetime_stats WHERE player_id = ?",
                    (player['id'],),
                ).fetchone()['c']
                if existing >= 15:
                    continue

                profile = scrape_player_profile_by_id(self.session, player['ll_id'], player['ll_username'])
                if profile and profile.get('categories'):
                    for cat in profile['categories']:
                        cat_id = categories.get(cat['name'])
                        if cat_id:
                            conn.execute("""
                                INSERT OR REPLACE INTO player_lifetime_stats
                                (player_id, category_id, correct_pct, total_questions)
                                VALUES (?, ?, ?, ?)
                            """, (player['id'], cat_id, cat['pct'], cat['total']))
                    scraped += 1
                    logger.info("  %s: %d categories", player['ll_username'], len(profile['categories']))

                if (i + 1) % 10 == 0:
                    conn.commit()

                time.sleep(1.0)

            conn.commit()

        if skipped_no_id:
            logger.warning("  Skipped %d players without LL ID", skipped_no_id)
        result.count("profiles", scraped)
        result.count("profiles_skipped_no_id", skipped_no_id)

    # ── Part 6: Rundle match day answers ───────────────────────────

    def _scrape_rundle_answers(
        self, season: int, rundle: str, rundle_id: int, result: ScrapeResult
    ) -> None:
        logger.info("[6/6] Scraping all players' answers from rundle match day pages...")

        with get_connection() as conn:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if not season_row:
                return
            season_id = season_row["id"]

            ll_id_to_player = {
                p['ll_id']: p['id']
                for p in conn.execute("SELECT id, ll_id FROM players WHERE ll_id IS NOT NULL").fetchall()
            }
            categories = {c['name']: c['id'] for c in conn.execute("SELECT id, name FROM categories").fetchall()}

            rundle_players = conn.execute("""
                SELECT p.id FROM players p
                JOIN player_rundles pr ON p.id = pr.player_id
                WHERE pr.rundle_id = ?
            """, (rundle_id,)).fetchall()
            rundle_player_count = len(rundle_players)

            total_answers = 0
            for day in range(1, 26):
                # Check existing coverage
                existing_count = conn.execute("""
                    SELECT COUNT(DISTINCT a.player_id) as c
                    FROM answers a
                    JOIN questions q ON a.question_id = q.id
                    JOIN player_rundles pr ON a.player_id = pr.player_id
                    WHERE q.season_id = ? AND q.match_day = ? AND pr.rundle_id = ?
                """, (season_id, day, rundle_id)).fetchone()['c']

                if existing_count >= rundle_player_count - 2:
                    logger.debug("  Day %d: already have %d/%d, skipping", day, existing_count, rundle_player_count)
                    continue

                data = scrape_rundle_matchday(self.session, season, day, rundle)
                if not data:
                    continue

                # Update questions
                for q in data.get('questions', []):
                    cat_abbrev = q.get('category', '').strip()
                    cat_name = CATEGORY_MAP.get(cat_abbrev, cat_abbrev)
                    cat_id = categories.get(cat_name)

                    conn.execute("""
                        UPDATE questions
                        SET question_text = COALESCE(?, question_text),
                            correct_answer = COALESCE(?, correct_answer),
                            category_id = COALESCE(?, category_id)
                        WHERE season_id = ? AND match_day = ? AND question_number = ?
                    """, (q.get('text'), q.get('answer'), cat_id, season_id, day, q['num']))

                q_num_to_id = {
                    q['question_number']: q['id']
                    for q in conn.execute(
                        "SELECT id, question_number FROM questions WHERE season_id = ? AND match_day = ?",
                        (season_id, day),
                    ).fetchall()
                }

                day_answers = 0
                for pa in data.get('player_answers', []):
                    player_id = ll_id_to_player.get(pa.get('ll_id'))
                    if not player_id:
                        continue

                    for q_num in range(1, 7):
                        q_id = q_num_to_id.get(q_num)
                        if not q_id:
                            continue
                        try:
                            conn.execute("""
                                INSERT OR REPLACE INTO answers
                                (player_id, question_id, correct, defense_points_assigned)
                                VALUES (?, ?, ?, ?)
                            """, (player_id, q_id, pa.get(f'q{q_num}_correct', False), pa.get(f'q{q_num}_defense', 0)))
                            day_answers += 1
                        except Exception:
                            pass

                conn.commit()
                total_answers += day_answers
                logger.info("  Day %d: %d answers", day, day_answers)

                time.sleep(1.0)

        result.count("rundle_answers", total_answers)

    # ── Simpler scrape_season (original runner interface) ──────────

    def scrape_season(
        self,
        season_number: int,
        include_questions: bool = True,
        include_matches: bool = True,
        include_player_details: bool = True,
        rundle_filter: Optional[str] = None,
    ) -> dict:
        """
        Scrape data for a season (original interface).

        Args:
            season_number: The LL season number to scrape
            include_questions: Whether to scrape question data
            include_matches: Whether to scrape match results
            include_player_details: Whether to scrape detailed player answers
            rundle_filter: Optional rundle to limit scraping to

        Returns:
            Summary dict
        """
        logger.info("=" * 50)
        logger.info("Scraping Season %d", season_number)
        logger.info("=" * 50)

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

            if include_questions:
                logger.info("[1/3] Scraping questions...")
                questions, _rundle_stats = scrape_season_questions(self.session, season_number)
                self._save_questions(conn, season_id, questions)
                summary["questions_scraped"] = len(questions)
                logger.info("  Saved %d questions", len(questions))

            if include_matches:
                logger.info("[2/3] Scraping match results...")
                for match_day in range(1, 26):
                    match_data = scrape_match_day(
                        self.session, season_number, match_day, rundle_filter
                    )
                    if match_data and match_data.get("matches"):
                        self._save_match_day(conn, season_id, match_data)
                        summary["matches_scraped"] += len(match_data["matches"])
                        logger.info("  Day %d: %d matches", match_day, len(match_data['matches']))

            if include_player_details and include_matches:
                logger.info("[3/3] Scraping player answer details...")
                players = conn.execute("""
                    SELECT DISTINCT p.id, p.ll_username
                    FROM players p
                    JOIN matches m ON p.id = m.player1_id OR p.id = m.player2_id
                    WHERE m.season_id = ?
                """, (season_id,)).fetchall()

                for player in players:
                    for match_day in range(1, 26):
                        answers = scrape_player_answers(
                            self.session, player["ll_username"], season_number, match_day
                        )
                        if answers and answers.get("questions"):
                            self._save_player_answers(conn, season_id, player["id"], match_day, answers)
                            summary["answers_scraped"] += len(answers["questions"])

                    summary["players_scraped"] += 1
                    if summary["players_scraped"] % 10 == 0:
                        logger.info("  Processed %d players...", summary['players_scraped'])

            conn.commit()

        summary["finished_at"] = datetime.now().isoformat()
        logger.info("=" * 50)
        logger.info("Scraping complete!")
        logger.info("  Questions: %d", summary['questions_scraped'])
        logger.info("  Matches: %d", summary['matches_scraped'])
        logger.info("  Players: %d", summary['players_scraped'])
        logger.info("  Answers: %d", summary['answers_scraped'])
        logger.info("=" * 50)

        return summary

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _get_question_map(conn: sqlite3.Connection, season_id: int) -> dict[tuple[int, int], int]:
        """Build (match_day, question_number) -> question_id mapping."""
        rows = conn.execute(
            "SELECT id, match_day, question_number FROM questions WHERE season_id = ?",
            (season_id,),
        ).fetchall()
        return {(r["match_day"], r["question_number"]): r["id"] for r in rows}

    def _save_questions(
        self, conn: sqlite3.Connection, season_id: int, questions: list[dict]
    ) -> None:
        """Save scraped questions to database."""
        for q in questions:
            category_id = get_category_id(conn, q.get("category", "Miscellaneous"))
            if not category_id:
                category_id = get_category_id(conn, "Miscellaneous")

            conn.execute("""
                INSERT OR REPLACE INTO questions
                (season_id, match_day, question_number, category_id, rundle_correct_pct, league_correct_pct, question_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                season_id,
                q.get("match_day"),
                q.get("number"),
                category_id,
                q.get("rundle_correct_pct"),
                q.get("league_correct_pct"),
                q.get("text"),
            ))

    def _save_match_day(
        self, conn: sqlite3.Connection, season_id: int, match_data: dict
    ) -> None:
        """Save match day results to database."""
        match_day = match_data.get("match_day")

        for match in match_data.get("matches", []):
            player1_id = get_or_create_player(conn, match["player1"])
            player2_id = get_or_create_player(conn, match["player2"])

            conn.execute("""
                INSERT INTO matches
                (season_id, match_day, player1_id, player2_id, player1_score, player2_score)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(season_id, match_day, player1_id, player2_id) DO UPDATE SET
                    player1_score = excluded.player1_score,
                    player2_score = excluded.player2_score
            """, (
                season_id,
                match_day,
                player1_id,
                player2_id,
                match.get("player1_score"),
                match.get("player2_score"),
            ))

    def _save_player_answers(
        self, conn: sqlite3.Connection, season_id: int, player_id: int,
        match_day: int, answers: dict
    ) -> None:
        """Save player's detailed answers to database."""
        for q in answers.get("questions", []):
            question = conn.execute(
                "SELECT id FROM questions WHERE season_id = ? AND match_day = ? AND question_number = ?",
                (season_id, match_day, q.get("number")),
            ).fetchone()

            if question:
                defense_points = None
                for d in answers.get("defense_received", []):
                    if d.get("question") == q.get("number"):
                        defense_points = d.get("points")
                        break

                conn.execute("""
                    INSERT OR REPLACE INTO answers
                    (player_id, question_id, correct, defense_points_assigned)
                    VALUES (?, ?, ?, ?)
                """, (player_id, question["id"], q.get("correct"), defense_points))

    def scrape_player(self, username: str) -> Optional[dict]:
        """Scrape a single player's profile."""
        return scrape_player_profile(self.session, username)

    def update_player_category_stats(
        self, conn: sqlite3.Connection, player_id: int, season_id: int,
        category_stats: dict
    ) -> None:
        """Update player's category statistics."""
        for category_name, pct in category_stats.items():
            category_id = get_category_id(conn, category_name)
            if category_id:
                conn.execute("""
                    INSERT OR REPLACE INTO player_category_stats
                    (player_id, category_id, season_id, correct_pct)
                    VALUES (?, ?, ?, ?)
                """, (player_id, category_id, season_id, pct))
