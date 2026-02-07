"""
Surprise Metric - Information-Theoretic Performance Analysis

This metric measures how much better or worse a player performs compared
to what we'd expect based on their historical category performance and
the question difficulty.

Uses information theory (log-odds combination and surprisal):
- Expected probability is computed by combining evidence from player history
  and question difficulty in log-odds space (like logistic regression)
- Surprise is measured as log2(1/P) for the observed outcome, signed by
  whether it was an over- or under-performance, then bias-corrected by
  subtracting the expected surprise given p so that a calibrated player
  averages exactly 0 regardless of skill level

A positive surprise means the player did better than expected.
A negative surprise means they did worse than expected.
"""

import sqlite3
from dataclasses import dataclass
from math import log, log2, exp

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric


@dataclass
class SurpriseScore:
    """Individual question surprise data."""
    match_day: int
    question_number: int
    category: str
    expected_prob: float
    actual: int  # 1 or 0
    surprise: float


@dataclass
class PlayerSurpriseSummary:
    """Summary of a player's surprise scores."""
    player_id: int
    username: str
    total_surprise: float
    avg_surprise: float
    questions_answered: int
    positive_surprises: int
    negative_surprises: int


def _logit(p: float) -> float:
    """Convert probability to log-odds."""
    p = max(0.001, min(0.999, p))  # Clamp to avoid log(0)
    return log(p / (1 - p))


def _inv_logit(x: float) -> float:
    """Convert log-odds back to probability."""
    return 1 / (1 + exp(-x))


def calculate_expected_probability(
    player_category_pct: float,
    question_difficulty: float,
    baseline: float = 0.5
) -> float:
    """
    Calculate expected probability using log-odds combination.

    This combines two sources of evidence:
    1. Player's historical performance in this category
    2. Question difficulty (% of rundle who got it right)

    The combination averages the two signals in log-odds space.
    We average rather than stack (add) because the signals are
    correlated — stacking assumes independence and produces
    overconfident predictions that bias surprise scores negative.

    Args:
        player_category_pct: Player's historical % in this category (0-1)
        question_difficulty: % of rundle who got this question right (0-1)
        baseline: Prior probability (typically 0.5)

    Returns:
        Expected probability of getting the question correct (0.01-0.99)
    """
    # Handle edge cases
    player_category_pct = player_category_pct or baseline
    question_difficulty = question_difficulty or baseline

    # Convert to log-odds and combine
    player_logit = _logit(player_category_pct)
    difficulty_logit = _logit(question_difficulty)

    # Average in log-odds space rather than stacking additively.
    # Stacking assumes the two signals are independent, but they're
    # correlated (category % already reflects question difficulty),
    # which leads to overconfident predictions and systematic negative bias.
    combined_logit = 0.5 * player_logit + 0.5 * difficulty_logit

    # Convert back to probability
    expected = _inv_logit(combined_logit)

    # Clamp to avoid extreme probabilities
    return max(0.01, min(0.99, expected))


def calculate_surprise(actual_correct: bool, expected_prob: float) -> float:
    """
    Calculate bias-corrected surprise score for a single question.

    Uses surprisal (self-information): -log2(P) measures how "surprising"
    an event is, signed by over/under performance. Then subtracts the
    expected surprise given p, so that a perfectly calibrated player
    averages exactly 0 regardless of skill level or question difficulty.

    Without this correction, E[surprise] = -p*log2(p) + (1-p)*log2(1-p),
    which is negative for p > 0.5 and positive for p < 0.5 — creating a
    systematic bias that confounds comparisons across skill levels.

    Args:
        actual_correct: Whether the player got it correct
        expected_prob: Expected probability of getting it correct

    Returns:
        Bias-corrected surprise score (positive = better than expected, negative = worse)
    """
    # Clamp to avoid log(0)
    p = max(0.001, min(0.999, expected_prob))

    if actual_correct:
        raw = -log2(p)
    else:
        raw = log2(1 - p)

    # Subtract expected surprise so E[adjusted] = 0 for calibrated players
    expected_surprise = p * (-log2(p)) + (1 - p) * log2(1 - p)

    return raw - expected_surprise


@metric
class SurpriseMetric(BaseMetric):
    """
    Measures performance vs historical expectation.

    High positive surprise over many questions could indicate:
    - Genuine improvement
    - Lucky streak
    - Anomalous behavior worth investigating

    High negative surprise could indicate:
    - Bad day / distraction
    - Declining performance
    """

    id = "surprise"
    name = "Performance Surprise"
    description = "How much better/worse than expected based on category history and question difficulty"
    scopes = [Scope.PLAYER, Scope.SEASON, Scope.RUNDLE]
    default_visualization = VisualizationType.LEADERBOARD
    cacheable = True
    cache_ttl = 1800  # 30 minutes

    # ── Public helpers (used by routes) ────────────────────────────

    def detail_for_player(
        self,
        conn: sqlite3.Connection,
        username: str,
        season_id: int,
        sort_by: str = "surprise",
        order: str = "desc",
    ) -> dict:
        """
        Per-question surprise breakdown for a player.

        Returns dict with player, season, total_surprise, avg_surprise,
        question_count, and a sortable questions list.
        """
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (username,)
        ).fetchone()
        if not player:
            return None

        answers = conn.execute("""
            SELECT
                a.correct,
                q.match_day,
                q.question_number,
                q.question_text,
                q.correct_answer,
                q.rundle_correct_pct,
                c.name as category,
                COALESCE(pcs.correct_pct, pls.correct_pct) as player_category_pct
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            JOIN categories c ON q.category_id = c.id
            LEFT JOIN player_category_stats pcs ON (
                pcs.player_id = a.player_id
                AND pcs.category_id = q.category_id
                AND pcs.season_id = q.season_id
            )
            LEFT JOIN player_lifetime_stats pls ON (
                pls.player_id = a.player_id
                AND pls.category_id = q.category_id
            )
            WHERE a.player_id = ? AND q.season_id = ?
            ORDER BY q.match_day, q.question_number
        """, (player["id"], season_id)).fetchall()

        questions = []
        total_surprise = 0

        for row in answers:
            player_cat_pct = row["player_category_pct"] or 0.5
            question_difficulty = row["rundle_correct_pct"] or 0.5

            expected = calculate_expected_probability(player_cat_pct, question_difficulty)
            surprise = calculate_surprise(row["correct"], expected)
            total_surprise += surprise

            questions.append({
                "match_day": row["match_day"],
                "question_number": row["question_number"],
                "category": row["category"],
                "question_text": row["question_text"] or "",
                "correct_answer": row["correct_answer"] or "",
                "got_correct": bool(row["correct"]),
                "expected_prob": round(expected, 3),
                "surprise": round(surprise, 3),
                "difficulty": round(question_difficulty, 3),
                "player_cat_pct": round(player_cat_pct, 3),
            })

        # Sort
        reverse = order.lower() == "desc"
        sort_keys = {
            "surprise": lambda x: x["surprise"],
            "match_day": lambda x: (x["match_day"], x["question_number"]),
            "category": lambda x: x["category"],
            "expected_prob": lambda x: x["expected_prob"],
        }
        if sort_by in sort_keys:
            questions.sort(key=sort_keys[sort_by], reverse=reverse)

        return {
            "player": username,
            "total_surprise": round(total_surprise, 3),
            "avg_surprise": round(total_surprise / len(questions), 4) if questions else 0,
            "question_count": len(questions),
            "questions": questions,
        }

    def distribution_by_day(
        self,
        conn: sqlite3.Connection,
        season_id: int,
        rundle: str | None = None,
        leverage_start_day: int = 12,
    ) -> dict:
        """
        Average surprise by match day, split by player leverage.

        Returns dict with distribution list and leverage metadata.
        """
        from ..config import Config

        # Get players with their ranks
        rundle_filter = ""
        params: list = [season_id]

        if rundle:
            rundle_row = conn.execute(
                "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
                (rundle, season_id),
            ).fetchone()
            if rundle_row:
                rundle_filter = "AND pr.rundle_id = ?"
                params.append(rundle_row["id"])

        players = conn.execute(f"""
            SELECT p.id, p.ll_username, pr.final_rank,
                   (SELECT COUNT(*) FROM player_rundles pr2 WHERE pr2.rundle_id = pr.rundle_id) as rundle_size
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            JOIN rundles r ON pr.rundle_id = r.id
            WHERE r.season_id = ? {rundle_filter}
        """, params).fetchall()

        # Classify leverage
        player_leverage = {}
        for p in players:
            rank = p["final_rank"] or 999
            size = p["rundle_size"] or 38
            pct = rank / size
            player_leverage[p["id"]] = "high" if (pct <= 0.2 or pct >= 0.8) else "low"

        # Calculate surprise by day
        daily_surprises: dict[int, dict[str, list]] = {}

        for p in players:
            player_id = p["id"]
            plev = player_leverage.get(player_id, "low")

            answers = conn.execute("""
                SELECT
                    a.correct,
                    q.match_day,
                    q.rundle_correct_pct,
                    COALESCE(pcs.correct_pct, pls.correct_pct) as player_category_pct
                FROM answers a
                JOIN questions q ON a.question_id = q.id
                LEFT JOIN player_category_stats pcs ON (
                    pcs.player_id = a.player_id
                    AND pcs.category_id = q.category_id
                    AND pcs.season_id = q.season_id
                )
                LEFT JOIN player_lifetime_stats pls ON (
                    pls.player_id = a.player_id
                    AND pls.category_id = q.category_id
                )
                WHERE a.player_id = ? AND q.season_id = ?
                ORDER BY q.match_day
            """, (player_id, season_id)).fetchall()

            for row in answers:
                day = row["match_day"]
                player_cat_pct = row["player_category_pct"] or 0.5
                question_difficulty = row["rundle_correct_pct"] or 0.5

                expected = calculate_expected_probability(player_cat_pct, question_difficulty)
                surprise = calculate_surprise(row["correct"], expected)

                if day not in daily_surprises:
                    daily_surprises[day] = {"all": [], "high": [], "low": []}

                daily_surprises[day]["all"].append(surprise)
                if day >= leverage_start_day:
                    daily_surprises[day][plev].append(surprise)

        # Build result
        distribution = []
        for day in sorted(daily_surprises.keys()):
            data = daily_surprises[day]
            entry = {
                "match_day": day,
                "avg_surprise_all": round(sum(data["all"]) / len(data["all"]), 4) if data["all"] else 0,
                "count_all": len(data["all"]),
            }
            if data["high"]:
                entry["avg_surprise_high"] = round(sum(data["high"]) / len(data["high"]), 4)
                entry["count_high"] = len(data["high"])
            if data["low"]:
                entry["avg_surprise_low"] = round(sum(data["low"]) / len(data["low"]), 4)
                entry["count_low"] = len(data["low"])
            distribution.append(entry)

        return {
            "distribution": distribution,
            "leverage_start_day": leverage_start_day,
            "leverage_explanation": {
                "high": "Players in top/bottom 20% of standings (promotion/relegation zone)",
                "low": "Players in middle 60% of standings (safely mid-table)",
                "note": f"Leverage split only applies after day {leverage_start_day} when standings stabilize",
            },
        }

    # ── Core calculate dispatch ────────────────────────────────────

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs
    ) -> MetricResult:
        """Calculate surprise metric for the given scope."""
        self.validate_scope(scope)

        if scope == Scope.PLAYER:
            return self._player_surprise(
                conn,
                kwargs["player_id"],
                kwargs.get("season_id")
            )
        elif scope == Scope.SEASON:
            return self._season_leaderboard(conn, kwargs["season_id"])
        elif scope == Scope.RUNDLE:
            return self._rundle_leaderboard(conn, kwargs["rundle_id"])

        raise ValueError(f"Unhandled scope: {scope}")

    def _player_surprise(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int | None = None
    ) -> MetricResult:
        """Calculate surprise scores for a specific player."""
        # Get player info
        player = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?",
            (player_id,)
        ).fetchone()

        if not player:
            raise ValueError(f"Player {player_id} not found")

        # Build query for player's answers with all needed context
        query = """
            SELECT
                a.correct,
                q.match_day,
                q.question_number,
                q.question_text,
                q.correct_answer,
                q.rundle_correct_pct,
                c.name as category,
                pcs.correct_pct as player_category_pct
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            JOIN categories c ON q.category_id = c.id
            LEFT JOIN player_category_stats pcs ON (
                pcs.player_id = a.player_id
                AND pcs.category_id = q.category_id
                AND pcs.season_id = q.season_id
            )
            WHERE a.player_id = ?
        """
        params = [player_id]

        if season_id:
            query += " AND q.season_id = ?"
            params.append(season_id)

        query += " ORDER BY q.match_day, q.question_number"

        rows = conn.execute(query, params).fetchall()

        # Calculate surprise for each question
        surprises = []
        total_surprise = 0.0

        for row in rows:
            player_cat_pct = row["player_category_pct"] or 0.5  # Default to 50% if no history
            question_difficulty = row["rundle_correct_pct"] or 0.5

            expected = calculate_expected_probability(
                player_cat_pct,
                question_difficulty
            )
            surprise = calculate_surprise(row["correct"], expected)
            total_surprise += surprise

            surprises.append({
                "match_day": row["match_day"],
                "question": row["question_number"],
                "category": row["category"],
                "expected": round(expected, 3),
                "actual": row["correct"],
                "surprise": round(surprise, 3),
                "cumulative": round(total_surprise, 3),
                "question_text": row["question_text"] or "",
                "correct_answer": row["correct_answer"] or "",
                "difficulty": round(question_difficulty, 3),
                "player_cat_pct": round(player_cat_pct, 3),
            })

        return MetricResult(
            metric_id=self.id,
            title=f"Surprise - {player['ll_username']}",
            description=self.description,
            data={
                "player": player["ll_username"],
                "total_surprise": round(total_surprise, 3),
                "avg_surprise": round(total_surprise / len(surprises), 3) if surprises else 0,
                "questions": len(surprises),
                "details": surprises,
            },
            visualization=VisualizationType.LINE_CHART,
            scope=Scope.PLAYER,
            columns=["Match Day", "Q#", "Category", "Expected", "Actual", "Surprise", "Cumulative"],
            chart_config={
                "xAxis": "match_day",
                "yAxis": "cumulative",
                "title": "Cumulative Surprise Over Season",
            },
        )

    def _season_leaderboard(
        self,
        conn: sqlite3.Connection,
        season_id: int
    ) -> MetricResult:
        """Calculate surprise leaderboard for a season."""
        # Get all players with answers in this season
        players = conn.execute(
            """
            SELECT DISTINCT p.id, p.ll_username
            FROM players p
            JOIN answers a ON a.player_id = p.id
            JOIN questions q ON a.question_id = q.id
            WHERE q.season_id = ?
            """,
            (season_id,)
        ).fetchall()

        leaderboard = []

        for player in players:
            result = self._player_surprise(conn, player["id"], season_id)
            data = result.data

            leaderboard.append({
                "rank": 0,  # Will be filled after sorting
                "username": player["ll_username"],
                "total_surprise": data["total_surprise"],
                "avg_surprise": data["avg_surprise"],
                "questions": data["questions"],
            })

        # Sort by total surprise descending
        leaderboard.sort(key=lambda x: x["total_surprise"], reverse=True)

        # Add ranks
        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i

        # Get season info
        season = conn.execute(
            "SELECT season_number FROM seasons WHERE id = ?",
            (season_id,)
        ).fetchone()

        return MetricResult(
            metric_id=self.id,
            title=f"Surprise Leaderboard - Season {season['season_number'] if season else season_id}",
            description="Players ranked by cumulative surprise (positive = outperforming expectations)",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.SEASON,
            columns=["Rank", "Player", "Total Surprise", "Avg Surprise", "Questions"],
        )

    def _rundle_leaderboard(
        self,
        conn: sqlite3.Connection,
        rundle_id: int
    ) -> MetricResult:
        """Calculate surprise leaderboard for a specific rundle."""
        # Get rundle info
        rundle = conn.execute(
            """
            SELECT r.*, s.season_number
            FROM rundles r
            JOIN seasons s ON r.season_id = s.id
            WHERE r.id = ?
            """,
            (rundle_id,)
        ).fetchone()

        if not rundle:
            raise ValueError(f"Rundle {rundle_id} not found")

        # Get players in this rundle
        players = conn.execute(
            """
            SELECT p.id, p.ll_username
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            WHERE pr.rundle_id = ?
            """,
            (rundle_id,)
        ).fetchall()

        leaderboard = []

        for player in players:
            result = self._player_surprise(conn, player["id"], rundle["season_id"])
            data = result.data

            leaderboard.append({
                "rank": 0,
                "username": player["ll_username"],
                "total_surprise": data["total_surprise"],
                "avg_surprise": data["avg_surprise"],
                "questions": data["questions"],
            })

        leaderboard.sort(key=lambda x: x["total_surprise"], reverse=True)

        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i

        return MetricResult(
            metric_id=self.id,
            title=f"Surprise - {rundle['name']} (Season {rundle['season_number']})",
            description=f"Rundle {rundle['level']} surprise leaderboard",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.RUNDLE,
            columns=["Rank", "Player", "Total Surprise", "Avg Surprise", "Questions"],
        )
