"""Metrics API endpoints - dynamic metric discovery and calculation."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection
from ...cache import response_cache
from ...metrics import MetricRegistry, Scope

router = APIRouter()


@router.get("")
async def list_metrics():
    """
    List all available metrics with their metadata.

    This endpoint enables dynamic discovery of metrics.
    The UI can use this to populate metric dropdowns.
    """
    metrics = MetricRegistry.all_info()
    return {
        "metrics": [m.to_dict() for m in metrics],
        "count": len(metrics),
    }


@router.get("/{metric_id}")
async def get_metric_info(metric_id: str):
    """
    Get detailed information about a specific metric.

    Args:
        metric_id: The metric identifier
    """
    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    return metric.get_info().to_dict()


@router.get("/{metric_id}/player/{username}")
async def calculate_player_metric(
    metric_id: str,
    username: str,
    season: Optional[int] = Query(None, description="Season number to filter by"),
    use_cache: bool = Query(True, description="Whether to use cached results"),
):
    """
    Calculate a metric for a specific player.

    Args:
        metric_id: The metric to calculate
        username: Player's LL username
        season: Optional season filter
        use_cache: Whether to use cached results
    """
    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    if Scope.PLAYER not in metric.scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Metric '{metric_id}' does not support player scope"
        )

    with get_connection() as conn:
        # Look up player
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?",
            (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        # Look up season if provided
        season_id = None
        if season:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?",
                (season,)
            ).fetchone()
            if season_row:
                season_id = season_row["id"]

        try:
            result = MetricRegistry.calculate(
                conn,
                metric_id,
                Scope.PLAYER,
                use_cache=use_cache,
                player_id=player["id"],
                season_id=season_id,
            )
            return result.to_dict()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/{metric_id}/season/{season_number}")
async def calculate_season_metric(
    metric_id: str,
    season_number: int,
    use_cache: bool = Query(True),
):
    """
    Calculate a metric for an entire season.

    Args:
        metric_id: The metric to calculate
        season_number: The season number
        use_cache: Whether to use cached results
    """
    cache_key = f"metric_season:{metric_id}:{season_number}"
    if use_cache:
        cached = response_cache.get(cache_key)
        if cached is not None:
            return cached

    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    if Scope.SEASON not in metric.scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Metric '{metric_id}' does not support season scope"
        )

    with get_connection() as conn:
        season = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?",
            (season_number,)
        ).fetchone()

        if not season:
            raise HTTPException(status_code=404, detail=f"Season {season_number} not found")

        try:
            result = MetricRegistry.calculate(
                conn,
                metric_id,
                Scope.SEASON,
                use_cache=use_cache,
                season_id=season["id"],
            )
            resp = result.to_dict()
            response_cache.set(cache_key, resp)
            return resp
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/{metric_id}/rundle/{rundle_id}")
async def calculate_rundle_metric(
    metric_id: str,
    rundle_id: int,
    use_cache: bool = Query(True),
):
    """
    Calculate a metric for a specific rundle.

    Args:
        metric_id: The metric to calculate
        rundle_id: The rundle ID
        use_cache: Whether to use cached results
    """
    cache_key = f"metric_rundle:{metric_id}:{rundle_id}"
    if use_cache:
        cached = response_cache.get(cache_key)
        if cached is not None:
            return cached

    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    if Scope.RUNDLE not in metric.scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Metric '{metric_id}' does not support rundle scope"
        )

    with get_connection() as conn:
        rundle = conn.execute("SELECT id FROM rundles WHERE id = ?", (rundle_id,)).fetchone()

        if not rundle:
            raise HTTPException(status_code=404, detail=f"Rundle {rundle_id} not found")

        try:
            result = MetricRegistry.calculate(
                conn,
                metric_id,
                Scope.RUNDLE,
                use_cache=use_cache,
                rundle_id=rundle_id,
            )
            resp = result.to_dict()
            response_cache.set(cache_key, resp)
            return resp
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/{metric_id}/h2h")
async def calculate_h2h_metric(
    metric_id: str,
    player1: str = Query(..., description="First player's username"),
    player2: str = Query(..., description="Second player's username"),
    season: Optional[int] = Query(None, description="Optional season filter"),
    use_cache: bool = Query(True),
):
    """
    Calculate a head-to-head metric between two players.

    Args:
        metric_id: The metric to calculate
        player1: First player's username
        player2: Second player's username
        season: Optional season filter
        use_cache: Whether to use cached results
    """
    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    if Scope.HEAD_TO_HEAD not in metric.scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Metric '{metric_id}' does not support head-to-head scope"
        )

    with get_connection() as conn:
        p1 = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (player1,)
        ).fetchone()
        p2 = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?", (player2,)
        ).fetchone()

        if not p1:
            raise HTTPException(status_code=404, detail=f"Player '{player1}' not found")
        if not p2:
            raise HTTPException(status_code=404, detail=f"Player '{player2}' not found")

        season_id = None
        if season:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if season_row:
                season_id = season_row["id"]

        try:
            result = MetricRegistry.calculate(
                conn,
                metric_id,
                Scope.HEAD_TO_HEAD,
                use_cache=use_cache,
                player1_id=p1["id"],
                player2_id=p2["id"],
                season_id=season_id,
            )
            return result.to_dict()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/{metric_id}/cache/clear")
async def clear_metric_cache(metric_id: str):
    """
    Clear cached results for a metric.

    Clears both the SQLite metric cache and the in-memory response cache.

    Args:
        metric_id: The metric to clear cache for, or "all" to clear everything
    """
    with get_connection() as conn:
        if metric_id == "all":
            count = MetricRegistry.clear_cache(conn)
            mem_count = response_cache.clear()
        else:
            metric = MetricRegistry.get(metric_id)
            if not metric:
                raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")
            count = MetricRegistry.clear_cache(conn, metric_id)
            mem_count = response_cache.clear(f"metric_season:{metric_id}:") + \
                        response_cache.clear(f"metric_rundle:{metric_id}:")

        return {"cleared": count, "memory_cleared": mem_count, "metric": metric_id}


@router.get("/compare")
async def compare_players(
    metric_id: str = Query(..., description="Metric to compare"),
    players: str = Query(..., description="Comma-separated usernames"),
    season: Optional[int] = Query(None, description="Season filter"),
):
    """
    Compare multiple players on a specific metric.

    Args:
        metric_id: The metric to use for comparison
        players: Comma-separated list of usernames
        season: Optional season filter
    """
    metric = MetricRegistry.get(metric_id)
    if not metric:
        raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")

    if Scope.PLAYER not in metric.scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Metric '{metric_id}' does not support player comparison"
        )

    usernames = [u.strip() for u in players.split(",") if u.strip()]
    if len(usernames) < 2:
        raise HTTPException(status_code=400, detail="Provide at least 2 players to compare")

    with get_connection() as conn:
        season_id = None
        if season:
            season_row = conn.execute(
                "SELECT id FROM seasons WHERE season_number = ?", (season,)
            ).fetchone()
            if season_row:
                season_id = season_row["id"]

        results = []
        for username in usernames:
            player = conn.execute(
                "SELECT id FROM players WHERE ll_username = ?", (username,)
            ).fetchone()

            if not player:
                results.append({"username": username, "error": "Player not found"})
                continue

            try:
                result = MetricRegistry.calculate(
                    conn,
                    metric_id,
                    Scope.PLAYER,
                    use_cache=True,
                    player_id=player["id"],
                    season_id=season_id,
                )
                results.append({
                    "username": username,
                    "result": result.to_dict(),
                })
            except Exception as e:
                results.append({"username": username, "error": str(e)})

        return {
            "metric": metric_id,
            "season": season,
            "comparisons": results,
        }
