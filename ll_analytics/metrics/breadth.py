"""
Category Breadth Metric - How balanced a player's category performance is.

A breadth score near 1.0 means consistent across all categories.
A breadth score near 0.0 means extreme specialization (strong in few, weak in many).
"""

import sqlite3
from statistics import stdev

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric


@metric
class CategoryBreadthMetric(BaseMetric):
    """
    Measures how evenly a player performs across all 18 LL categories.

    Breadth = 1 - (stdev(category_pcts) / 0.5), clamped to [0, 1].
    Higher = more balanced performance across categories.
    """

    id = "breadth"
    name = "Category Breadth"
    description = "How balanced your performance is across all categories (1.0 = perfectly even)"
    scopes = [Scope.PLAYER, Scope.RUNDLE]
    default_visualization = VisualizationType.BAR_CHART
    cacheable = True
    cache_ttl = 1800

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs,
    ) -> MetricResult:
        self.validate_scope(scope)

        if scope == Scope.PLAYER:
            return self._player_breadth(
                conn, kwargs["player_id"], kwargs.get("season_id")
            )
        elif scope == Scope.RUNDLE:
            return self._rundle_breadth_leaderboard(conn, kwargs["rundle_id"])

        raise ValueError(f"Unhandled scope: {scope}")

    def _get_category_profile(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int | None = None,
    ) -> list[dict]:
        """Get category stats for a player, filtered to categories with >= 5 questions."""
        if season_id:
            rows = conn.execute("""
                SELECT c.name, pcs.correct_pct, pcs.total_questions
                FROM player_category_stats pcs
                JOIN categories c ON pcs.category_id = c.id
                WHERE pcs.player_id = ? AND pcs.season_id = ?
                ORDER BY pcs.correct_pct DESC
            """, (player_id, season_id)).fetchall()

        if not season_id or not rows:
            rows = conn.execute("""
                SELECT c.name, pls.correct_pct, pls.total_questions
                FROM player_lifetime_stats pls
                JOIN categories c ON pls.category_id = c.id
                WHERE pls.player_id = ?
                ORDER BY pls.correct_pct DESC
            """, (player_id,)).fetchall()

        return [
            {"name": r["name"], "pct": r["correct_pct"], "questions": r["total_questions"]}
            for r in rows
            if (r["total_questions"] or 0) >= 5
        ]

    def _compute_breadth(self, profile: list[dict]) -> dict:
        """Compute breadth score and identify strongest/weakest categories."""
        if len(profile) < 3:
            return {
                "breadth_score": 0,
                "strongest": [],
                "weakest": [],
                "profile": profile,
                "error": "Not enough categories with sufficient data",
            }

        pcts = [c["pct"] for c in profile]
        sd = stdev(pcts) if len(pcts) > 1 else 0.0
        breadth_score = max(0.0, min(1.0, 1.0 - (sd / 0.5)))

        sorted_by_pct = sorted(profile, key=lambda x: x["pct"], reverse=True)
        strongest = sorted_by_pct[:3]
        weakest = sorted_by_pct[-3:]

        return {
            "breadth_score": round(breadth_score, 3),
            "stdev": round(sd, 4),
            "categories_used": len(profile),
            "strongest": [
                {"name": c["name"], "pct": round(c["pct"] * 100, 1)}
                for c in strongest
            ],
            "weakest": [
                {"name": c["name"], "pct": round(c["pct"] * 100, 1)}
                for c in weakest
            ],
            "profile": [
                {"name": c["name"], "pct": round(c["pct"] * 100, 1), "questions": c["questions"]}
                for c in sorted_by_pct
            ],
        }

    def _player_breadth(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int | None = None,
    ) -> MetricResult:
        player = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?", (player_id,)
        ).fetchone()
        if not player:
            raise ValueError(f"Player {player_id} not found")

        if not season_id:
            season = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_id = season["id"] if season else None

        profile = self._get_category_profile(conn, player_id, season_id)
        data = self._compute_breadth(profile)
        data["player"] = player["ll_username"]

        return MetricResult(
            metric_id=self.id,
            title=f"Category Breadth - {player['ll_username']}",
            description=self.description,
            data=data,
            visualization=VisualizationType.BAR_CHART,
            scope=Scope.PLAYER,
        )

    def _rundle_breadth_leaderboard(
        self,
        conn: sqlite3.Connection,
        rundle_id: int,
    ) -> MetricResult:
        rundle = conn.execute("""
            SELECT r.*, s.season_number, s.id as season_id
            FROM rundles r JOIN seasons s ON r.season_id = s.id
            WHERE r.id = ?
        """, (rundle_id,)).fetchone()
        if not rundle:
            raise ValueError(f"Rundle {rundle_id} not found")

        players = conn.execute("""
            SELECT p.id, p.ll_username
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            WHERE pr.rundle_id = ?
        """, (rundle_id,)).fetchall()

        leaderboard = []
        for p in players:
            try:
                profile = self._get_category_profile(conn, p["id"], rundle["season_id"])
                result = self._compute_breadth(profile)
                if "error" not in result:
                    leaderboard.append({
                        "rank": 0,
                        "username": p["ll_username"],
                        "breadth_score": result["breadth_score"],
                        "stdev": result["stdev"],
                        "categories": result["categories_used"],
                    })
            except Exception:
                pass

        leaderboard.sort(key=lambda x: x["breadth_score"], reverse=True)
        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i

        return MetricResult(
            metric_id=self.id,
            title=f"Breadth Leaderboard - {rundle['name']}",
            description="Players ranked by category breadth (higher = more balanced)",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.RUNDLE,
            columns=["Rank", "Player", "Breadth Score", "Std Dev", "Categories"],
        )
