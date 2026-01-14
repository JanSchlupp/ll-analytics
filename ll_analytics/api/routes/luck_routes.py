"""Luck metric API routes."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection
from ...metrics.luck import LuckMetric, Scope

router = APIRouter()


# Use a different path to avoid collision with {username} parameter
@router.get("/luck-leaderboard")
async def get_luck_leaderboard(
    season: int = Query(107, description="Season number"),
    rundle: str = Query(..., description="Rundle name"),
):
    """Get luck leaderboard for a rundle."""
    with get_connection() as conn:
        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()

        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        rundle_row = conn.execute(
            "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
            (rundle, season_row["id"])
        ).fetchone()

        if not rundle_row:
            raise HTTPException(status_code=404, detail=f"Rundle '{rundle}' not found")

        metric = LuckMetric()
        result = metric.calculate(conn, Scope.RUNDLE, rundle_id=rundle_row["id"])

        return {"rundle": rundle, "season": season, "leaderboard": result.data}


@router.get("/luck/{username}")
async def get_player_luck(
    username: str,
    season: int = Query(107, description="Season number"),
):
    """Get luck analysis for a specific player."""
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()

        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        metric = LuckMetric()
        result = metric.calculate(conn, Scope.PLAYER, player_id=player["id"], season_id=season_row["id"])

        return result.data


@router.get("/match-detail/{username}/{match_day}")
async def get_match_detail(
    username: str,
    match_day: int,
    season: int = Query(107, description="Season number"),
):
    """
    Get detailed question-by-question breakdown for a specific match.

    Returns each question's category, CA%, both players' results, and their
    lifetime stats in that category.
    """
    with get_connection() as conn:
        # Get player
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (username,)
        ).fetchone()
        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        # Get season
        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()
        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        # Find the match
        match = conn.execute("""
            SELECT m.*, p1.ll_username as p1_name, p2.ll_username as p2_name
            FROM matches m
            JOIN players p1 ON m.player1_id = p1.id
            JOIN players p2 ON m.player2_id = p2.id
            WHERE m.season_id = ? AND m.match_day = ?
            AND (m.player1_id = ? OR m.player2_id = ?)
        """, (season_row["id"], match_day, player["id"], player["id"])).fetchone()

        if not match:
            raise HTTPException(status_code=404, detail=f"Match not found for day {match_day}")

        # Determine which player is "you" vs "opponent"
        if match["player1_id"] == player["id"]:
            your_id = match["player1_id"]
            your_name = match["p1_name"]
            opp_id = match["player2_id"]
            opp_name = match["p2_name"]
            your_score = match["player1_score"]
            opp_score = match["player2_score"]
            your_tca = match["player1_tca"]
            opp_tca = match["player2_tca"]
            your_field = "player1"
            opp_field = "player2"
        else:
            your_id = match["player2_id"]
            your_name = match["p2_name"]
            opp_id = match["player1_id"]
            opp_name = match["p1_name"]
            your_score = match["player2_score"]
            opp_score = match["player1_score"]
            your_tca = match["player2_tca"]
            opp_tca = match["player1_tca"]
            your_field = "player2"
            opp_field = "player1"

        # Get per-question data
        questions = conn.execute("""
            SELECT mq.*, c.name as category_name
            FROM match_questions mq
            LEFT JOIN categories c ON mq.category_id = c.id
            WHERE mq.match_id = ?
            ORDER BY mq.question_num
        """, (match["id"],)).fetchall()

        # Get lifetime stats for both players
        your_lifetime = {r["category_id"]: r for r in conn.execute("""
            SELECT * FROM player_lifetime_stats WHERE player_id = ?
        """, (your_id,)).fetchall()}

        opp_lifetime = {r["category_id"]: r for r in conn.execute("""
            SELECT * FROM player_lifetime_stats WHERE player_id = ?
        """, (opp_id,)).fetchall()}

        # Build question details
        question_details = []
        for q in questions:
            cat_id = q["category_id"]
            your_cat_pct = your_lifetime.get(cat_id, {}).get("correct_pct") if cat_id else None
            opp_cat_pct = opp_lifetime.get(cat_id, {}).get("correct_pct") if cat_id else None

            question_details.append({
                "question_num": q["question_num"],
                "category": q["category_name"],
                "ca_pct": q["question_ca_pct"],
                "your_correct": q[f"{your_field}_correct"],
                "opp_correct": q[f"{opp_field}_correct"],
                "your_defense": q[f"{your_field}_defense"],
                "opp_defense": q[f"{opp_field}_defense"],
                "your_cat_pct": round(your_cat_pct * 100, 1) if your_cat_pct else None,
                "opp_cat_pct": round(opp_cat_pct * 100, 1) if opp_cat_pct else None,
            })

        result = "W" if your_score > opp_score else ("L" if your_score < opp_score else "T")

        return {
            "match_day": match_day,
            "your_name": your_name,
            "opponent": opp_name,
            "your_score": your_score,
            "opp_score": opp_score,
            "your_tca": your_tca,
            "opp_tca": opp_tca,
            "result": result,
            "questions": question_details,
        }
