"""Player-related API endpoints."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection

router = APIRouter()


@router.get("")
async def list_players(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    search: Optional[str] = None,
):
    """
    List all players.

    Args:
        limit: Maximum number of players to return
        offset: Number of players to skip
        search: Optional search term for username
    """
    with get_connection() as conn:
        query = "SELECT id, ll_username, display_name FROM players"
        params = []

        if search:
            query += " WHERE ll_username LIKE ?"
            params.append(f"%{search}%")

        query += " ORDER BY ll_username LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()

        # Get total count
        count_query = "SELECT COUNT(*) as count FROM players"
        if search:
            count_query += " WHERE ll_username LIKE ?"
            count_params = [f"%{search}%"]
        else:
            count_params = []

        total = conn.execute(count_query, count_params).fetchone()["count"]

        return {
            "players": [dict(row) for row in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@router.get("/{username}")
async def get_player(username: str):
    """
    Get a player's profile and statistics.

    Args:
        username: Player's LL username
    """
    with get_connection() as conn:
        player = conn.execute(
            "SELECT * FROM players WHERE ll_username = ?",
            (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        player_data = dict(player)

        # Get category stats
        category_stats = conn.execute(
            """
            SELECT c.name as category, pcs.correct_pct, pcs.total_questions, s.season_number
            FROM player_category_stats pcs
            JOIN categories c ON pcs.category_id = c.id
            JOIN seasons s ON pcs.season_id = s.id
            WHERE pcs.player_id = ?
            ORDER BY s.season_number DESC, c.name
            """,
            (player["id"],)
        ).fetchall()

        player_data["category_stats"] = [dict(row) for row in category_stats]

        # Get rundle history
        rundles = conn.execute(
            """
            SELECT r.league, r.level, r.name, pr.final_rank, s.season_number
            FROM player_rundles pr
            JOIN rundles r ON pr.rundle_id = r.id
            JOIN seasons s ON r.season_id = s.id
            WHERE pr.player_id = ?
            ORDER BY s.season_number DESC
            """,
            (player["id"],)
        ).fetchall()

        player_data["rundle_history"] = [dict(row) for row in rundles]

        # Get recent performance summary
        recent = conn.execute(
            """
            SELECT
                COUNT(*) as total_questions,
                SUM(CASE WHEN a.correct THEN 1 ELSE 0 END) as correct_answers,
                AVG(CASE WHEN a.correct THEN 1.0 ELSE 0.0 END) as correct_pct
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE a.player_id = ?
            """,
            (player["id"],)
        ).fetchone()

        player_data["performance_summary"] = dict(recent) if recent else None

        return player_data


@router.get("/{username}/matches")
async def get_player_matches(
    username: str,
    season: Optional[int] = None,
    limit: int = Query(50, ge=1, le=500),
):
    """
    Get a player's match history.

    Args:
        username: Player's LL username
        season: Optional season filter
        limit: Maximum matches to return
    """
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?",
            (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        query = """
            SELECT
                m.match_day,
                s.season_number,
                p1.ll_username as player1,
                p2.ll_username as player2,
                m.player1_score,
                m.player2_score,
                m.player1_tca,
                m.player2_tca,
                CASE WHEN m.player1_id = ? THEN 'player1' ELSE 'player2' END as user_side
            FROM matches m
            JOIN seasons s ON m.season_id = s.id
            JOIN players p1 ON m.player1_id = p1.id
            JOIN players p2 ON m.player2_id = p2.id
            WHERE m.player1_id = ? OR m.player2_id = ?
        """
        params = [player["id"], player["id"], player["id"]]

        if season:
            query += " AND s.season_number = ?"
            params.append(season)

        query += " ORDER BY s.season_number DESC, m.match_day DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        matches = []
        for row in rows:
            match = dict(row)
            # Normalize so user is always "you"
            if match["user_side"] == "player2":
                match["opponent"] = match["player1"]
                match["your_score"] = match["player2_score"]
                match["opponent_score"] = match["player1_score"]
            else:
                match["opponent"] = match["player2"]
                match["your_score"] = match["player1_score"]
                match["opponent_score"] = match["player2_score"]
            matches.append(match)

        return {"username": username, "matches": matches}


@router.get("/{username}/answers")
async def get_player_answers(
    username: str,
    season: Optional[int] = None,
    match_day: Optional[int] = None,
):
    """
    Get a player's detailed answer history.

    Args:
        username: Player's LL username
        season: Optional season filter
        match_day: Optional match day filter
    """
    with get_connection() as conn:
        player = conn.execute(
            "SELECT id FROM players WHERE ll_username = ?",
            (username,)
        ).fetchone()

        if not player:
            raise HTTPException(status_code=404, detail=f"Player '{username}' not found")

        query = """
            SELECT
                q.match_day,
                q.question_number,
                c.name as category,
                a.correct,
                a.defense_points_assigned,
                q.rundle_correct_pct,
                s.season_number
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            JOIN categories c ON q.category_id = c.id
            JOIN seasons s ON q.season_id = s.id
            WHERE a.player_id = ?
        """
        params = [player["id"]]

        if season:
            query += " AND s.season_number = ?"
            params.append(season)

        if match_day:
            query += " AND q.match_day = ?"
            params.append(match_day)

        query += " ORDER BY s.season_number DESC, q.match_day, q.question_number"

        rows = conn.execute(query, params).fetchall()

        return {
            "username": username,
            "answers": [dict(row) for row in rows],
        }
