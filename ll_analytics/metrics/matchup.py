"""
Head-to-Head Matchup Predictor Metric.

Predicts the outcome of a match between two players based on their category
profiles and the category distribution of questions in the season.

Uses normal approximation for win probability (no scipy needed).
"""

import sqlite3
import math

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric


def _norm_cdf(x: float) -> float:
    """Standard normal CDF using math.erf. No scipy needed."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@metric
class MatchupPredictorMetric(BaseMetric):
    """
    Predicts head-to-head matchup outcomes between two players.

    Uses category profiles and question frequency to estimate expected TCA
    for each player, then computes win probability via normal approximation.
    """

    id = "matchup"
    name = "Matchup Predictor"
    description = "Predicted outcome between two players based on category strengths"
    scopes = [Scope.HEAD_TO_HEAD]
    default_visualization = VisualizationType.BAR_CHART
    cacheable = False

    def predict(
        self,
        conn: sqlite3.Connection,
        player1_id: int,
        player2_id: int,
        season_id: int,
    ) -> dict:
        """
        Predict matchup between two players.

        Returns dict with expected TCA, win probability, and category advantages.
        """
        # Get player names
        p1 = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?", (player1_id,)
        ).fetchone()
        p2 = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?", (player2_id,)
        ).fetchone()
        if not p1 or not p2:
            return {"error": "Player not found"}

        # Get category profiles (prefer season-specific, fall back to lifetime)
        p1_cats = self._get_category_profile(conn, player1_id, season_id)
        p2_cats = self._get_category_profile(conn, player2_id, season_id)

        # Get category question frequency for the season (weights)
        cat_freq = {}
        freq_rows = conn.execute("""
            SELECT c.name, COUNT(*) as cnt
            FROM questions q
            JOIN categories c ON q.category_id = c.id
            WHERE q.season_id = ?
            GROUP BY c.name
        """, (season_id,)).fetchall()

        total_questions = sum(r["cnt"] for r in freq_rows) or 1
        for r in freq_rows:
            cat_freq[r["name"]] = r["cnt"] / total_questions

        # Per-category expected correct and variance
        all_categories = set(list(p1_cats.keys()) + list(p2_cats.keys()) + list(cat_freq.keys()))

        p1_expected_tca = 0.0
        p2_expected_tca = 0.0
        p1_variance = 0.0
        p2_variance = 0.0
        category_advantages = []

        for cat in all_categories:
            weight = cat_freq.get(cat, 0)
            if weight == 0:
                continue

            p1_pct = p1_cats.get(cat, 0.5)
            p2_pct = p2_cats.get(cat, 0.5)

            # Expected correct for 6 questions weighted by category frequency
            p1_exp = p1_pct * weight * 6
            p2_exp = p2_pct * weight * 6
            p1_expected_tca += p1_exp
            p2_expected_tca += p2_exp

            # Variance: binomial variance = p*(1-p)*n*weight
            p1_variance += p1_pct * (1 - p1_pct) * weight * 6
            p2_variance += p2_pct * (1 - p2_pct) * weight * 6

            advantage = p1_pct - p2_pct
            category_advantages.append({
                "category": cat,
                "p1_pct": round(p1_pct * 100, 1),
                "p2_pct": round(p2_pct * 100, 1),
                "advantage": round(advantage * 100, 1),
                "weight": round(weight * 100, 1),
            })

        # Sort advantages by magnitude (biggest advantage for p1 first)
        category_advantages.sort(key=lambda x: x["advantage"], reverse=True)

        # Win probability via normal approximation
        # P(p1_tca > p2_tca) using combined variance
        diff_mean = p1_expected_tca - p2_expected_tca
        combined_variance = p1_variance + p2_variance
        combined_std = max(combined_variance ** 0.5, 0.01)

        p1_win_prob = _norm_cdf(diff_mean / combined_std)
        p2_win_prob = 1.0 - p1_win_prob

        return {
            "player1": p1["ll_username"],
            "player2": p2["ll_username"],
            "p1_expected_tca": round(p1_expected_tca, 2),
            "p2_expected_tca": round(p2_expected_tca, 2),
            "p1_win_prob": round(p1_win_prob, 3),
            "p2_win_prob": round(p2_win_prob, 3),
            "category_advantages": category_advantages,
        }

    def _get_category_profile(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int,
    ) -> dict[str, float]:
        """Get player's category percentages as {category_name: pct}."""
        # Try season-specific first
        rows = conn.execute("""
            SELECT c.name, pcs.correct_pct
            FROM player_category_stats pcs
            JOIN categories c ON pcs.category_id = c.id
            WHERE pcs.player_id = ? AND pcs.season_id = ?
        """, (player_id, season_id)).fetchall()

        if not rows:
            rows = conn.execute("""
                SELECT c.name, pls.correct_pct
                FROM player_lifetime_stats pls
                JOIN categories c ON pls.category_id = c.id
                WHERE pls.player_id = ?
            """, (player_id,)).fetchall()

        return {r["name"]: r["correct_pct"] for r in rows}

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs,
    ) -> MetricResult:
        self.validate_scope(scope)

        player1_id = kwargs["player1_id"]
        player2_id = kwargs["player2_id"]
        season_id = kwargs.get("season_id")

        if not season_id:
            season = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_id = season["id"] if season else None
        if not season_id:
            raise ValueError("No season found")

        data = self.predict(conn, player1_id, player2_id, season_id)

        return MetricResult(
            metric_id=self.id,
            title=f"Matchup: {data.get('player1', '?')} vs {data.get('player2', '?')}",
            description=self.description,
            data=data,
            visualization=VisualizationType.BAR_CHART,
            scope=Scope.HEAD_TO_HEAD,
        )
