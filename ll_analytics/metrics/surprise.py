"""
Surprise Metric - Information-Theoretic Performance Analysis

This metric measures how much better or worse a player performs compared
to what we'd expect based on their historical category performance and
the question difficulty.

Uses information theory (log-odds combination and surprisal):
- Expected probability is computed by combining evidence from player history
  and question difficulty in log-odds space (like logistic regression)
- Surprise is measured as log2(1/P) for the observed outcome, signed by
  whether it was an over- or under-performance

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

    The combination happens in log-odds space, which properly handles
    probabilities and avoids the problems of simple averaging.

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
    # Each source contributes evidence relative to the baseline
    player_logit = _logit(player_category_pct)
    difficulty_logit = _logit(question_difficulty)
    baseline_logit = _logit(baseline)

    # Combined log-odds: each source adds evidence beyond baseline
    # This is like a simplified logistic regression
    combined_logit = player_logit + difficulty_logit - baseline_logit

    # Convert back to probability
    expected = _inv_logit(combined_logit)

    # Clamp to avoid extreme probabilities
    return max(0.01, min(0.99, expected))


def calculate_surprise(actual_correct: bool, expected_prob: float) -> float:
    """
    Calculate information-theoretic surprise score for a single question.

    Uses surprisal (self-information): -log2(P) measures how "surprising"
    an event is. We sign it based on over/under performance:
    - Correct when unlikely (P low) -> large positive surprise
    - Wrong when likely (P high) -> large negative surprise

    Args:
        actual_correct: Whether the player got it correct
        expected_prob: Expected probability of getting it correct

    Returns:
        Signed surprise score (positive = better than expected, negative = worse)
    """
    # Clamp to avoid log(0)
    p = max(0.001, min(0.999, expected_prob))

    if actual_correct:
        # Got it right - surprise is how unlikely that was
        # More surprising (positive) when expected_prob was low
        return -log2(p)  # Positive: ~0.01 (p≈1) to ~10 (p≈0.001)
    else:
        # Got it wrong - surprise is how unlikely THAT was
        # More surprising (negative) when expected_prob was HIGH (you should've got it)
        return log2(1 - p)  # Negative: ~-10 (p≈0.999) to ~-0.01 (p≈0.001)


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
