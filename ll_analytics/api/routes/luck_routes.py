"""Luck metric API routes."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection
from ...metrics.luck import LuckMetric, Scope

router = APIRouter()

_luck = LuckMetric()


def _resolve_season(conn, season: int | None) -> tuple[int, dict]:
    """Resolve a season number to a row, using DB latest as fallback."""
    if season is None:
        row = conn.execute(
            "SELECT id, season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No seasons found")
        return row["season_number"], row
    row = conn.execute(
        "SELECT id, season_number FROM seasons WHERE season_number = ?", (season,)
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Season {season} not found")
    return row["season_number"], row


# Use a different path to avoid collision with {username} parameter
@router.get("/luck-leaderboard")
async def get_luck_leaderboard(
    season: int = Query(default=None, description="Season number"),
    rundle: str = Query(..., description="Rundle name"),
):
    """Get luck leaderboard for a rundle."""
    with get_connection() as conn:
        season_num, season_row = _resolve_season(conn, season)

        rundle_row = conn.execute(
            "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
            (rundle, season_row["id"])
        ).fetchone()

        if not rundle_row:
            raise HTTPException(status_code=404, detail=f"Rundle '{rundle}' not found")

        result = _luck.calculate(conn, Scope.RUNDLE, rundle_id=rundle_row["id"])

        return {"rundle": rundle, "season": season_num, "leaderboard": result.data}


@router.get("/luck/{username}")
async def get_player_luck(
    username: str,
    season: int = Query(default=None, description="Season number"),
):
    """Get luck analysis for a specific player."""
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        _, season_row = _resolve_season(conn, season)

        result = _luck.calculate(conn, Scope.PLAYER, player_id=player["id"], season_id=season_row["id"])

        return result.data


@router.get("/match-detail/{username}/{match_day}")
async def get_match_detail(
    username: str,
    match_day: int,
    season: int = Query(default=None, description="Season number"),
):
    """
    Get detailed question-by-question breakdown for a specific match.
    """
    with get_connection() as conn:
        _, season_row = _resolve_season(conn, season)

        result = _luck.match_detail(conn, username, match_day, season_row["id"])

        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Match not found for '{username}' on day {match_day}",
            )

        return result
