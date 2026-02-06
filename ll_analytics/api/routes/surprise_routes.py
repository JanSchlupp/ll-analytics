"""Additional surprise metric routes for detailed analysis."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...config import Config
from ...database import get_connection
from ...metrics.surprise import SurpriseMetric

router = APIRouter()

_surprise = SurpriseMetric()


@router.get("/surprise/distribution")
async def surprise_distribution(
    season: int = Query(..., description="Season number"),
    rundle: Optional[str] = Query(None, description="Rundle name to filter by"),
):
    """
    Get surprise distribution over time (average surprise by match day).

    Split by player leverage (only after day 12 when standings stabilize).
    """
    with get_connection() as conn:
        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()

        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        result = _surprise.distribution_by_day(
            conn,
            season_row["id"],
            rundle=rundle,
            leverage_start_day=Config.LEVERAGE_START_DAY,
        )

        return {"season": season, "rundle": rundle, **result}


@router.get("/surprise/questions/{username}")
async def surprise_questions(
    username: str,
    season: int = Query(..., description="Season number"),
    sort_by: str = Query("surprise", description="Sort by: surprise, match_day, category"),
    order: str = Query("desc", description="Sort order: asc or desc"),
):
    """
    Get per-question surprise breakdown for a player.
    Returns sortable list with question text and contribution to total surprise.
    """
    with get_connection() as conn:
        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()

        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        result = _surprise.detail_for_player(
            conn, username, season_row["id"], sort_by=sort_by, order=order
        )

        if result is None:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        return {"season": season, **result}
