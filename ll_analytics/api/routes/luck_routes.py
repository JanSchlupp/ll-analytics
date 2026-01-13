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
