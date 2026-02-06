"""Heatmap and dashboard data API endpoints."""

from typing import Optional
from fastapi import APIRouter, Query

from ...database import get_connection
from ...metrics.surprise import calculate_expected_probability, calculate_surprise

router = APIRouter()


@router.get("/api/players/{username}/heatmap")
async def player_heatmap(username: str, season: Optional[int] = Query(None)):
    """
    Player performance heatmap data.

    Returns {data: {day: {qnum: {correct, category, question_text}}}}
    """
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (username,)
        ).fetchone()
        if not player:
            return {"error": "Player not found", "data": {}}

        if season:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
        else:
            season_row = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()

        if not season_row:
            return {"error": "Season not found", "data": {}}

        rows = conn.execute("""
            SELECT
                a.correct,
                q.match_day,
                q.question_number,
                q.question_text,
                c.name as category
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            JOIN categories c ON q.category_id = c.id
            WHERE a.player_id = ? AND q.season_id = ?
            ORDER BY q.match_day, q.question_number
        """, (player["id"], season_row["id"])).fetchall()

        data = {}
        for r in rows:
            day = str(r["match_day"])
            if day not in data:
                data[day] = {}
            data[day][str(r["question_number"])] = {
                "correct": bool(r["correct"]),
                "category": r["category"],
                "question_text": r["question_text"] or "",
            }

        return {"data": data}


@router.get("/api/categories/heatmap")
async def category_heatmap(season: Optional[int] = Query(None)):
    """
    Category difficulty heatmap data.

    Returns {data: {category: {day: avg_ca_pct}}}
    """
    with get_connection() as conn:
        if season:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
        else:
            season_row = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()

        if not season_row:
            return {"error": "Season not found", "data": {}}

        rows = conn.execute("""
            SELECT
                c.name as category,
                q.match_day,
                AVG(q.rundle_correct_pct) as avg_ca_pct
            FROM questions q
            JOIN categories c ON q.category_id = c.id
            WHERE q.season_id = ?
            GROUP BY c.name, q.match_day
            ORDER BY c.name, q.match_day
        """, (season_row["id"],)).fetchall()

        data = {}
        for r in rows:
            cat = r["category"]
            if cat not in data:
                data[cat] = {}
            data[cat][str(r["match_day"])] = round(r["avg_ca_pct"] or 0, 3)

        return {"data": data}


@router.get("/api/dashboard")
async def dashboard_data(
    season_id: Optional[int] = Query(None),
    rundle_id: Optional[int] = Query(None),
):
    """
    Dashboard widget data: movers, category difficulty, quick stats.
    """
    with get_connection() as conn:
        if not season_id:
            season = conn.execute(
                "SELECT id FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_id = season["id"] if season else None

        if not season_id:
            return {"movers": [], "category_difficulty": {}, "stats": {}}

        # Find rundle if not provided
        if not rundle_id:
            from ...config import Config
            rundle = conn.execute(
                "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
                (Config.DEFAULT_RUNDLE, season_id)
            ).fetchone()
            if rundle:
                rundle_id = rundle["id"]

        # --- Movers & Shakers ---
        movers = []
        if rundle_id:
            players = conn.execute("""
                SELECT p.id, p.ll_username
                FROM players p
                JOIN player_rundles pr ON p.id = pr.player_id
                WHERE pr.rundle_id = ?
            """, (rundle_id,)).fetchall()

            # Get max match day
            max_day_row = conn.execute("""
                SELECT MAX(match_day) as max_day FROM matches WHERE season_id = ?
            """, (season_id,)).fetchone()
            max_day = max_day_row["max_day"] if max_day_row and max_day_row["max_day"] else 25

            for p in players:
                recent_avg = _get_avg_surprise_for_days(
                    conn, p["id"], season_id, max(1, max_day - 4), max_day
                )
                earlier_avg = _get_avg_surprise_for_days(
                    conn, p["id"], season_id, max(1, max_day - 9), max(1, max_day - 5)
                )
                if recent_avg is not None and earlier_avg is not None:
                    delta = recent_avg - earlier_avg
                    movers.append({
                        "username": p["ll_username"],
                        "delta": round(delta, 3),
                        "recent_avg": round(recent_avg, 3),
                    })

            movers.sort(key=lambda x: x["delta"], reverse=True)
            movers = movers[:3]

        # --- Category Difficulty ---
        cat_rows = conn.execute("""
            SELECT c.name, AVG(q.rundle_correct_pct) as avg_pct
            FROM questions q
            JOIN categories c ON q.category_id = c.id
            WHERE q.season_id = ?
            GROUP BY c.name
            ORDER BY avg_pct DESC
        """, (season_id,)).fetchall()

        category_difficulty = {
            r["name"]: round(r["avg_pct"] or 0, 3) for r in cat_rows
        }

        # --- Quick Stats ---
        match_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM matches WHERE season_id = ?",
            (season_id,)
        ).fetchone()

        question_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM questions WHERE season_id = ?",
            (season_id,)
        ).fetchone()

        max_day_row = conn.execute(
            "SELECT MAX(match_day) as max_day FROM matches WHERE season_id = ?",
            (season_id,)
        ).fetchone()
        max_day = max_day_row["max_day"] if max_day_row and max_day_row["max_day"] else 0
        season_progress = round(max_day / 25 * 100)

        stats = {
            "totalMatches": match_count["cnt"] if match_count else 0,
            "totalQuestions": question_count["cnt"] if question_count else 0,
            "seasonProgress": season_progress,
        }

        return {
            "movers": movers,
            "category_difficulty": category_difficulty,
            "stats": stats,
        }


def _get_avg_surprise_for_days(
    conn,
    player_id: int,
    season_id: int,
    day_min: int,
    day_max: int,
) -> float | None:
    """Calculate average surprise for a player over a range of match days."""
    rows = conn.execute("""
        SELECT
            a.correct,
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
        AND q.match_day >= ? AND q.match_day <= ?
    """, (player_id, season_id, day_min, day_max)).fetchall()

    if not rows:
        return None

    total = 0.0
    for r in rows:
        player_cat_pct = r["player_category_pct"] or 0.5
        difficulty = r["rundle_correct_pct"] or 0.5
        expected = calculate_expected_probability(player_cat_pct, difficulty)
        total += calculate_surprise(r["correct"], expected)

    return total / len(rows)
