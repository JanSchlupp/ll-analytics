"""
Late Season Spike Metric

Compares performance in early season (days 1-10) vs late season (days 20-25)
to detect unusual improvements that could indicate:
- Increased effort when stakes are higher
- Genuine improvement over time
- Anomalous behavior worth investigating
"""

import sqlite3
import statistics
from dataclasses import dataclass

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric
from .surprise import calculate_expected_probability, calculate_surprise
from ..config import Config


@dataclass
class PeriodStats:
    """Statistics for a period of the season."""
    total_surprise: float
    avg_surprise: float
    questions: int
    correct: int
    correct_pct: float


@metric
class LateSeasonSpikeMetric(BaseMetric):
    """
    Detects unusual late-season performance changes.

    Compares match days 1-10 (early) vs 20-25 (late).
    A high positive delta indicates late-season improvement.
    """

    id = "late_spike"
    name = "Late-Season Spike"
    description = "Compare early season (days 1-10) vs late season (days 20-25) performance"
    scopes = [Scope.PLAYER, Scope.SEASON]
    default_visualization = VisualizationType.LEADERBOARD
    cacheable = True
    cache_ttl = 3600

    EARLY_DAYS = Config.EARLY_DAYS
    LATE_DAYS = Config.LATE_DAYS

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs
    ) -> MetricResult:
        self.validate_scope(scope)

        if scope == Scope.PLAYER:
            return self._player_spike(
                conn,
                kwargs["player_id"],
                kwargs.get("season_id")
            )
        elif scope == Scope.SEASON:
            return self._season_spike_leaderboard(conn, kwargs["season_id"])

        raise ValueError(f"Unhandled scope: {scope}")

    def _get_period_surprises(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int,
        day_range: range
    ) -> list[float]:
        """Get surprise scores for a specific period."""
        query = """
            SELECT
                a.correct,
                q.rundle_correct_pct,
                pcs.correct_pct as player_category_pct
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            LEFT JOIN player_category_stats pcs ON (
                pcs.player_id = a.player_id
                AND pcs.category_id = q.category_id
                AND pcs.season_id = q.season_id
            )
            WHERE a.player_id = ?
            AND q.season_id = ?
            AND q.match_day BETWEEN ? AND ?
        """
        rows = conn.execute(
            query,
            (player_id, season_id, min(day_range), max(day_range))
        ).fetchall()

        surprises = []
        for row in rows:
            player_cat_pct = row["player_category_pct"] or 0.5
            question_difficulty = row["rundle_correct_pct"] or 0.5

            expected = calculate_expected_probability(player_cat_pct, question_difficulty)
            surprise = calculate_surprise(row["correct"], expected)
            surprises.append(surprise)

        return surprises

    def _player_spike(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int | None = None
    ) -> MetricResult:
        """Calculate late-season spike for a specific player."""
        player = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?",
            (player_id,)
        ).fetchone()

        if not player:
            raise ValueError(f"Player {player_id} not found")

        # If no season specified, use most recent
        if not season_id:
            season = conn.execute(
                """
                SELECT DISTINCT q.season_id
                FROM answers a
                JOIN questions q ON a.question_id = q.id
                WHERE a.player_id = ?
                ORDER BY q.season_id DESC
                LIMIT 1
                """,
                (player_id,)
            ).fetchone()
            if season:
                season_id = season["season_id"]
            else:
                raise ValueError(f"No data found for player {player_id}")

        early_surprises = self._get_period_surprises(
            conn, player_id, season_id, self.EARLY_DAYS
        )
        late_surprises = self._get_period_surprises(
            conn, player_id, season_id, self.LATE_DAYS
        )

        early_avg = statistics.mean(early_surprises) if early_surprises else 0
        late_avg = statistics.mean(late_surprises) if late_surprises else 0
        delta = late_avg - early_avg

        # Calculate z-score if we have enough data
        z_score = None
        if len(early_surprises) >= 5 and len(late_surprises) >= 5:
            early_std = statistics.stdev(early_surprises) if len(early_surprises) > 1 else 0.1
            if early_std > 0:
                z_score = delta / early_std

        return MetricResult(
            metric_id=self.id,
            title=f"Late-Season Spike - {player['ll_username']}",
            description=self.description,
            data={
                "player": player["ll_username"],
                "season_id": season_id,
                "early": {
                    "days": f"{min(self.EARLY_DAYS)}-{max(self.EARLY_DAYS)}",
                    "total_surprise": round(sum(early_surprises), 3),
                    "avg_surprise": round(early_avg, 3),
                    "questions": len(early_surprises),
                },
                "late": {
                    "days": f"{min(self.LATE_DAYS)}-{max(self.LATE_DAYS)}",
                    "total_surprise": round(sum(late_surprises), 3),
                    "avg_surprise": round(late_avg, 3),
                    "questions": len(late_surprises),
                },
                "delta": round(delta, 3),
                "z_score": round(z_score, 2) if z_score else None,
                "significant": z_score is not None and abs(z_score) > 1.96,  # 95% CI
            },
            visualization=VisualizationType.BAR_CHART,
            scope=Scope.PLAYER,
            chart_config={
                "type": "comparison",
                "labels": ["Early Season", "Late Season"],
                "title": "Average Surprise: Early vs Late Season",
            },
        )

    def _season_spike_leaderboard(
        self,
        conn: sqlite3.Connection,
        season_id: int
    ) -> MetricResult:
        """Generate leaderboard of late-season spikes."""
        # Get all players in this season
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
            try:
                result = self._player_spike(conn, player["id"], season_id)
                data = result.data

                # Only include if we have data for both periods
                if data["early"]["questions"] > 0 and data["late"]["questions"] > 0:
                    leaderboard.append({
                        "rank": 0,
                        "username": player["ll_username"],
                        "early_avg": data["early"]["avg_surprise"],
                        "late_avg": data["late"]["avg_surprise"],
                        "delta": data["delta"],
                        "z_score": data["z_score"],
                        "significant": data["significant"],
                    })
            except ValueError:
                continue

        # Sort by delta descending (biggest late-season improvements first)
        leaderboard.sort(key=lambda x: x["delta"], reverse=True)

        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i

        season = conn.execute(
            "SELECT season_number FROM seasons WHERE id = ?",
            (season_id,)
        ).fetchone()

        return MetricResult(
            metric_id=self.id,
            title=f"Late-Season Spike - Season {season['season_number'] if season else season_id}",
            description="Players ranked by late-season performance improvement (positive = improved late)",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.SEASON,
            columns=["Rank", "Player", "Early Avg", "Late Avg", "Delta", "Z-Score", "Significant"],
        )
