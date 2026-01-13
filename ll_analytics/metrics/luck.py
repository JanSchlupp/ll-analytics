"""
Luck Metric - Opponent Performance Variance

This metric measures how "lucky" or "unlucky" a player was based on
how their opponents performed against them compared to their season average.

If opponents consistently performed above their average against you -> unlucky
If opponents consistently performed below their average against you -> lucky

The surprise-weighted version also considers how *surprising* the opponent's
performance was, giving more weight to unusually good/bad opponent days.
"""

import sqlite3
from math import log2
from dataclasses import dataclass
from typing import Optional

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric


@dataclass
class OpponentMatch:
    """Data about a single opponent matchup."""
    match_day: int
    opponent: str
    opponent_tca: int
    opponent_avg_tca: float
    your_tca: int
    your_score: int
    opponent_score: int
    luck_contribution: float
    surprise_weighted_luck: float


def calculate_opponent_luck(
    opponent_tca: int,
    opponent_avg_tca: float,
    opponent_std_tca: float = 1.0
) -> tuple[float, float]:
    """
    Calculate luck contribution from a single opponent matchup.

    Args:
        opponent_tca: How many correct answers opponent got this match
        opponent_avg_tca: Opponent's season average TCA per match
        opponent_std_tca: Opponent's standard deviation (for surprise weighting)

    Returns:
        Tuple of (raw_luck, surprise_weighted_luck)
        Positive = lucky for you (opponent underperformed)
        Negative = unlucky for you (opponent overperformed)
    """
    # Raw luck: how much did opponent deviate from their average?
    raw_luck = opponent_avg_tca - opponent_tca  # Positive if they underperformed

    # Surprise weighting: how unusual was this performance?
    if opponent_std_tca > 0:
        z_score = abs(opponent_tca - opponent_avg_tca) / opponent_std_tca
        # Use log scaling to avoid extreme weights
        surprise_factor = 1 + log2(1 + z_score)
    else:
        surprise_factor = 1.0

    surprise_weighted_luck = raw_luck * surprise_factor

    return raw_luck, surprise_weighted_luck


@metric
class LuckMetric(BaseMetric):
    """
    Measures opponent performance variance ("luck").

    High positive luck means opponents consistently underperformed against you.
    High negative luck means opponents consistently overperformed against you.

    This metric requires match data to be populated in the database.
    """

    id = "luck"
    name = "Opponent Luck"
    description = "How much opponents over/underperformed their average against you"
    scopes = [Scope.PLAYER, Scope.RUNDLE]
    default_visualization = VisualizationType.LEADERBOARD
    cacheable = True
    cache_ttl = 1800

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs
    ) -> MetricResult:
        """Calculate luck metric for the given scope."""
        self.validate_scope(scope)

        if scope == Scope.PLAYER:
            return self._player_luck(
                conn,
                kwargs["player_id"],
                kwargs.get("season_id")
            )
        elif scope == Scope.RUNDLE:
            return self._rundle_leaderboard(conn, kwargs["rundle_id"])

        raise ValueError(f"Unhandled scope: {scope}")

    def _get_player_tca_stats(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int
    ) -> tuple[float, float]:
        """Get a player's average and std deviation of TCA per match."""
        # Get TCA per match day from matches table
        rows = conn.execute("""
            SELECT
                CASE WHEN player1_id = ? THEN player1_tca ELSE player2_tca END as tca
            FROM matches
            WHERE season_id = ? AND (player1_id = ? OR player2_id = ?)
        """, (player_id, season_id, player_id, player_id)).fetchall()

        if not rows:
            return 3.0, 1.0  # Default values

        tcas = [r['tca'] for r in rows if r['tca'] is not None]
        if not tcas:
            return 3.0, 1.0

        avg = sum(tcas) / len(tcas)
        if len(tcas) > 1:
            variance = sum((t - avg) ** 2 for t in tcas) / (len(tcas) - 1)
            std = variance ** 0.5
        else:
            std = 1.0

        return avg, max(std, 0.5)  # Minimum std to avoid division issues

    def _player_luck(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: Optional[int] = None
    ) -> MetricResult:
        """Calculate luck for a specific player."""
        player = conn.execute(
            "SELECT ll_username FROM players WHERE id = ?",
            (player_id,)
        ).fetchone()

        if not player:
            raise ValueError(f"Player {player_id} not found")

        if not season_id:
            season = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_id = season['id'] if season else None

        if not season_id:
            raise ValueError("No season found")

        # Get all matches for this player
        matches = conn.execute("""
            SELECT
                m.match_day,
                m.player1_id, m.player2_id,
                m.player1_tca, m.player2_tca,
                m.player1_score, m.player2_score,
                p1.ll_username as player1_name,
                p2.ll_username as player2_name
            FROM matches m
            JOIN players p1 ON m.player1_id = p1.id
            JOIN players p2 ON m.player2_id = p2.id
            WHERE m.season_id = ? AND (m.player1_id = ? OR m.player2_id = ?)
            ORDER BY m.match_day
        """, (season_id, player_id, player_id)).fetchall()

        if not matches:
            return MetricResult(
                metric_id=self.id,
                title=f"Luck - {player['ll_username']}",
                description="No match data available",
                data={"error": "No matches found"},
                visualization=VisualizationType.TEXT,
                scope=Scope.PLAYER,
            )

        # Pre-compute opponent stats
        opponent_stats = {}

        match_details = []
        total_luck = 0.0
        total_weighted_luck = 0.0

        for m in matches:
            # Determine who is the opponent
            if m['player1_id'] == player_id:
                opponent_id = m['player2_id']
                opponent_name = m['player2_name']
                opponent_tca = m['player2_tca']
                your_tca = m['player1_tca']
                your_score = m['player1_score']
                opponent_score = m['player2_score']
            else:
                opponent_id = m['player1_id']
                opponent_name = m['player1_name']
                opponent_tca = m['player1_tca']
                your_tca = m['player2_tca']
                your_score = m['player2_score']
                opponent_score = m['player1_score']

            # Get opponent's average TCA
            if opponent_id not in opponent_stats:
                opponent_stats[opponent_id] = self._get_player_tca_stats(
                    conn, opponent_id, season_id
                )

            opp_avg, opp_std = opponent_stats[opponent_id]

            # Calculate luck contribution
            raw_luck, weighted_luck = calculate_opponent_luck(
                opponent_tca, opp_avg, opp_std
            )

            total_luck += raw_luck
            total_weighted_luck += weighted_luck

            match_details.append({
                "match_day": m['match_day'],
                "opponent": opponent_name,
                "opponent_tca": opponent_tca,
                "opponent_avg": round(opp_avg, 2),
                "your_tca": your_tca,
                "your_score": your_score,
                "opponent_score": opponent_score,
                "result": "W" if your_score > opponent_score else ("L" if your_score < opponent_score else "T"),
                "luck": round(raw_luck, 3),
                "weighted_luck": round(weighted_luck, 3),
            })

        return MetricResult(
            metric_id=self.id,
            title=f"Luck - {player['ll_username']}",
            description=self.description,
            data={
                "player": player['ll_username'],
                "total_luck": round(total_luck, 3),
                "total_weighted_luck": round(total_weighted_luck, 3),
                "avg_luck_per_match": round(total_luck / len(matches), 3) if matches else 0,
                "matches_played": len(matches),
                "details": match_details,
            },
            visualization=VisualizationType.LINE_CHART,
            scope=Scope.PLAYER,
            columns=["Day", "Opponent", "Opp TCA", "Opp Avg", "Result", "Luck"],
            chart_config={
                "xAxis": "match_day",
                "yAxis": "cumulative_luck",
                "title": "Cumulative Luck Over Season",
            },
        )

    def _rundle_leaderboard(
        self,
        conn: sqlite3.Connection,
        rundle_id: int
    ) -> MetricResult:
        """Calculate luck leaderboard for a rundle."""
        rundle = conn.execute("""
            SELECT r.*, s.season_number, s.id as season_id
            FROM rundles r
            JOIN seasons s ON r.season_id = s.id
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

        for player in players:
            try:
                result = self._player_luck(conn, player['id'], rundle['season_id'])
                data = result.data
                if 'error' not in data:
                    leaderboard.append({
                        "rank": 0,
                        "username": player['ll_username'],
                        "total_luck": data['total_luck'],
                        "weighted_luck": data['total_weighted_luck'],
                        "matches": data['matches_played'],
                    })
            except Exception:
                pass

        leaderboard.sort(key=lambda x: x['total_luck'], reverse=True)
        for i, entry in enumerate(leaderboard, 1):
            entry['rank'] = i

        return MetricResult(
            metric_id=self.id,
            title=f"Luck Leaderboard - {rundle['name']}",
            description="Players ranked by opponent luck (positive = lucky)",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.RUNDLE,
            columns=["Rank", "Player", "Total Luck", "Weighted Luck", "Matches"],
        )
