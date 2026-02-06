"""
Defense Strategy Metric - How effectively a player uses defensive point allocation.

Analyzes defense distribution patterns, effectiveness when assigning high defense,
targeting intelligence (correlation with opponent weakness), and net ROI.
"""

import sqlite3
from statistics import mean

from .base import BaseMetric, MetricResult, Scope, VisualizationType
from .registry import metric


def _gini_coefficient(values: list[float]) -> float:
    """Calculate Gini coefficient (0=equal, 1=concentrated) for a distribution."""
    if not values or all(v == 0 for v in values):
        return 0.0
    n = len(values)
    sorted_vals = sorted(values)
    total = sum(sorted_vals)
    if total == 0:
        return 0.0
    cumulative = 0.0
    gini_sum = 0.0
    for i, v in enumerate(sorted_vals):
        cumulative += v
        gini_sum += (2 * (i + 1) - n - 1) * v
    return gini_sum / (n * total)


def calculate_defense_effectiveness(
    defense_pts: int,
    opp_correct: bool,
    opp_category_pct: float | None,
) -> dict:
    """
    Calculate effectiveness of a single defensive assignment.

    Args:
        defense_pts: Points assigned to defend this question
        opp_correct: Whether the opponent got it correct
        opp_category_pct: Opponent's lifetime % in this category (0-1), or None

    Returns:
        Dict with defense_pts, opp_correct, opp_category_pct, points_saved
    """
    # Points saved: if opponent got it wrong AND you assigned defense, those pts
    # didn't count. The "saved" value = defense_pts when opponent missed.
    points_saved = defense_pts if not opp_correct else 0

    return {
        "defense_pts": defense_pts,
        "opp_correct": opp_correct,
        "opp_category_pct": opp_category_pct,
        "points_saved": points_saved,
    }


@metric
class DefenseStrategyMetric(BaseMetric):
    """
    Analyzes how a player distributes and targets their 6 defense points.

    Measures:
    - Allocation pattern (Gini): concentrated vs spread defense
    - Effectiveness: opponent miss rate on high-defense questions vs baseline
    - Targeting intelligence: correlation with opponent category weakness
    - ROI: net points gained from defense strategy
    """

    id = "defense"
    name = "Defense Strategy"
    description = "How effectively you allocate your 6 defense points each match"
    scopes = [Scope.PLAYER, Scope.RUNDLE]
    default_visualization = VisualizationType.BAR_CHART
    cacheable = True
    cache_ttl = 1800

    def defense_detail_for_player(
        self,
        conn: sqlite3.Connection,
        player_id: int,
        season_id: int,
    ) -> dict:
        """
        Detailed defense analysis for a single player.

        Returns dict with allocation_gini, effectiveness, targeting, roi, and per-match details.
        """
        # Get all match_questions where this player was involved
        # When player is player1: player2_defense = defense assigned TO player1 by player2
        #   and player1_defense = defense assigned TO player2 by player1 (what WE assigned)
        # When player is player2: player1_defense = defense assigned TO player2 by player2
        #   and player2_defense = defense assigned TO player1 by player2 (what WE assigned)
        rows = conn.execute("""
            SELECT
                mq.question_num,
                mq.category_id,
                mq.question_ca_pct,
                mq.player1_correct,
                mq.player2_correct,
                mq.player1_defense,
                mq.player2_defense,
                m.match_day,
                m.player1_id,
                m.player2_id,
                c.name as category_name
            FROM match_questions mq
            JOIN matches m ON mq.match_id = m.id
            LEFT JOIN categories c ON mq.category_id = c.id
            WHERE m.season_id = ? AND (m.player1_id = ? OR m.player2_id = ?)
            ORDER BY m.match_day, mq.question_num
        """, (season_id, player_id, player_id)).fetchall()

        if not rows:
            return {
                "error": "No match data found",
                "allocation_gini": 0,
                "effectiveness": 0,
                "targeting_score": 0,
                "roi": 0,
            }

        # Get opponent lifetime stats for targeting analysis
        opponent_ids = set()
        for r in rows:
            if r["player1_id"] == player_id:
                opponent_ids.add(r["player2_id"])
            else:
                opponent_ids.add(r["player1_id"])

        opp_lifetime = {}
        for opp_id in opponent_ids:
            stats = conn.execute(
                "SELECT category_id, correct_pct FROM player_lifetime_stats WHERE player_id = ?",
                (opp_id,)
            ).fetchall()
            opp_lifetime[opp_id] = {s["category_id"]: s["correct_pct"] for s in stats}

        all_defense_pts = []
        high_defense_opp_wrong = 0
        high_defense_total = 0
        baseline_opp_wrong = 0
        baseline_total = 0
        targeting_pairs = []  # (defense_assigned, opp_weakness)
        total_points_saved = 0
        total_points_lost = 0

        for r in rows:
            if r["player1_id"] == player_id:
                # We are player1; our defense assignment is player1_defense
                # player1_defense = defense pts player2 assigned TO player1
                # player2_defense = defense pts player1 assigned TO player2
                our_defense = r["player2_defense"] or 0  # what we assigned to opponent's question
                opp_correct = r["player2_correct"]
                opp_id = r["player2_id"]
            else:
                # We are player2; our defense assignment is player2_defense
                # player1_defense = defense pts player2 assigned TO player1
                # player2_defense = defense pts player1 assigned TO player2
                our_defense = r["player1_defense"] or 0  # what we assigned to opponent's question
                opp_correct = r["player1_correct"]
                opp_id = r["player1_id"]

            all_defense_pts.append(our_defense)

            # Effectiveness: when we assign 2+ defense, how often does opponent miss?
            if our_defense >= 2:
                high_defense_total += 1
                if not opp_correct:
                    high_defense_opp_wrong += 1
            else:
                baseline_total += 1
                if not opp_correct:
                    baseline_opp_wrong += 1

            # Targeting: correlation between defense assigned and opponent weakness
            cat_id = r["category_id"]
            if cat_id and opp_id in opp_lifetime and cat_id in opp_lifetime[opp_id]:
                opp_cat_pct = opp_lifetime[opp_id][cat_id]
                opp_weakness = 1.0 - opp_cat_pct  # higher = weaker
                targeting_pairs.append((our_defense, opp_weakness))

            # ROI: points saved vs points lost
            if not opp_correct:
                total_points_saved += our_defense
            else:
                total_points_lost += our_defense

        # Calculate metrics
        allocation_gini = _gini_coefficient(all_defense_pts)

        high_defense_miss_rate = (
            high_defense_opp_wrong / high_defense_total
            if high_defense_total > 0 else 0
        )
        baseline_miss_rate = (
            baseline_opp_wrong / baseline_total
            if baseline_total > 0 else 0
        )
        effectiveness = high_defense_miss_rate - baseline_miss_rate

        # Targeting: simple correlation
        targeting_score = 0.0
        if len(targeting_pairs) >= 5:
            def_vals = [p[0] for p in targeting_pairs]
            weak_vals = [p[1] for p in targeting_pairs]
            def_mean = mean(def_vals)
            weak_mean = mean(weak_vals)
            numerator = sum(
                (d - def_mean) * (w - weak_mean)
                for d, w in zip(def_vals, weak_vals)
            )
            denom_d = sum((d - def_mean) ** 2 for d in def_vals) ** 0.5
            denom_w = sum((w - weak_mean) ** 2 for w in weak_vals) ** 0.5
            if denom_d > 0 and denom_w > 0:
                targeting_score = numerator / (denom_d * denom_w)

        roi = total_points_saved - total_points_lost

        return {
            "allocation_gini": round(allocation_gini, 3),
            "effectiveness": round(effectiveness, 3),
            "high_defense_miss_rate": round(high_defense_miss_rate, 3),
            "baseline_miss_rate": round(baseline_miss_rate, 3),
            "high_defense_questions": high_defense_total,
            "targeting_score": round(targeting_score, 3),
            "roi": roi,
            "total_points_saved": total_points_saved,
            "total_points_lost": total_points_lost,
            "questions_analyzed": len(rows),
        }

    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs,
    ) -> MetricResult:
        self.validate_scope(scope)

        if scope == Scope.PLAYER:
            return self._player_defense(
                conn, kwargs["player_id"], kwargs.get("season_id")
            )
        elif scope == Scope.RUNDLE:
            return self._rundle_defense_leaderboard(conn, kwargs["rundle_id"])

        raise ValueError(f"Unhandled scope: {scope}")

    def _player_defense(
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
        if not season_id:
            raise ValueError("No season found")

        data = self.defense_detail_for_player(conn, player_id, season_id)
        data["player"] = player["ll_username"]

        return MetricResult(
            metric_id=self.id,
            title=f"Defense Strategy - {player['ll_username']}",
            description=self.description,
            data=data,
            visualization=VisualizationType.BAR_CHART,
            scope=Scope.PLAYER,
            columns=["Metric", "Value"],
        )

    def _rundle_defense_leaderboard(
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
                detail = self.defense_detail_for_player(
                    conn, p["id"], rundle["season_id"]
                )
                if "error" not in detail:
                    leaderboard.append({
                        "rank": 0,
                        "username": p["ll_username"],
                        "effectiveness": detail["effectiveness"],
                        "roi": detail["roi"],
                        "targeting": detail["targeting_score"],
                        "gini": detail["allocation_gini"],
                    })
            except Exception:
                pass

        leaderboard.sort(key=lambda x: x["roi"], reverse=True)
        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i

        return MetricResult(
            metric_id=self.id,
            title=f"Defense Leaderboard - {rundle['name']}",
            description="Players ranked by defense ROI",
            data=leaderboard,
            visualization=VisualizationType.LEADERBOARD,
            scope=Scope.RUNDLE,
            columns=["Rank", "Player", "Effectiveness", "ROI", "Targeting", "Gini"],
        )
