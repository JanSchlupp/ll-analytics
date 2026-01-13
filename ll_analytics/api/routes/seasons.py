"""Season-related API endpoints."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection

router = APIRouter()


@router.get("")
async def list_seasons():
    """List all seasons with summary statistics."""
    with get_connection() as conn:
        seasons = conn.execute(
            """
            SELECT
                s.id,
                s.season_number,
                s.start_date,
                s.end_date,
                COUNT(DISTINCT q.id) as question_count,
                COUNT(DISTINCT m.id) as match_count,
                COUNT(DISTINCT CASE WHEN m.player1_id IS NOT NULL THEN m.player1_id END) +
                COUNT(DISTINCT CASE WHEN m.player2_id IS NOT NULL THEN m.player2_id END) as player_count
            FROM seasons s
            LEFT JOIN questions q ON s.id = q.season_id
            LEFT JOIN matches m ON s.id = m.season_id
            GROUP BY s.id
            ORDER BY s.season_number DESC
            """
        ).fetchall()

        return {"seasons": [dict(row) for row in seasons]}


@router.get("/{season_number}")
async def get_season(season_number: int):
    """
    Get details for a specific season.

    Args:
        season_number: The season number
    """
    with get_connection() as conn:
        season = conn.execute(
            "SELECT * FROM seasons WHERE season_number = ?",
            (season_number,)
        ).fetchone()

        if not season:
            raise HTTPException(status_code=404, detail=f"Season {season_number} not found")

        season_data = dict(season)

        # Get question stats by category
        categories = conn.execute(
            """
            SELECT
                c.name as category,
                COUNT(*) as question_count,
                AVG(q.rundle_correct_pct) as avg_correct_pct
            FROM questions q
            JOIN categories c ON q.category_id = c.id
            WHERE q.season_id = ?
            GROUP BY c.id
            ORDER BY c.name
            """,
            (season["id"],)
        ).fetchall()

        season_data["categories"] = [dict(row) for row in categories]

        # Get match day summary
        match_days = conn.execute(
            """
            SELECT
                match_day,
                COUNT(*) as match_count
            FROM matches
            WHERE season_id = ?
            GROUP BY match_day
            ORDER BY match_day
            """,
            (season["id"],)
        ).fetchall()

        season_data["match_days"] = [dict(row) for row in match_days]

        return season_data


@router.get("/{season_number}/rundles")
async def get_season_rundles(season_number: int):
    """
    Get all rundles for a season.

    Args:
        season_number: The season number
    """
    with get_connection() as conn:
        season = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?",
            (season_number,)
        ).fetchone()

        if not season:
            raise HTTPException(status_code=404, detail=f"Season {season_number} not found")

        rundles = conn.execute(
            """
            SELECT
                r.id,
                r.league,
                r.level,
                r.name,
                COUNT(pr.player_id) as player_count
            FROM rundles r
            LEFT JOIN player_rundles pr ON r.id = pr.rundle_id
            WHERE r.season_id = ?
            GROUP BY r.id
            ORDER BY r.league, r.level
            """,
            (season["id"],)
        ).fetchall()

        return {"season": season_number, "rundles": [dict(row) for row in rundles]}


@router.get("/{season_number}/rundles/{rundle_id}")
async def get_rundle_standings(season_number: int, rundle_id: int):
    """
    Get standings for a specific rundle.

    Args:
        season_number: The season number
        rundle_id: The rundle ID
    """
    with get_connection() as conn:
        rundle = conn.execute(
            """
            SELECT r.*, s.season_number
            FROM rundles r
            JOIN seasons s ON r.season_id = s.id
            WHERE r.id = ? AND s.season_number = ?
            """,
            (rundle_id, season_number)
        ).fetchone()

        if not rundle:
            raise HTTPException(
                status_code=404,
                detail=f"Rundle {rundle_id} not found in season {season_number}"
            )

        standings = conn.execute(
            """
            SELECT
                p.ll_username,
                pr.final_rank,
                (SELECT COUNT(*) FROM answers a
                 JOIN questions q ON a.question_id = q.id
                 WHERE a.player_id = p.id AND q.season_id = ?) as total_questions,
                (SELECT SUM(CASE WHEN a.correct THEN 1 ELSE 0 END) FROM answers a
                 JOIN questions q ON a.question_id = q.id
                 WHERE a.player_id = p.id AND q.season_id = ?) as correct_answers
            FROM player_rundles pr
            JOIN players p ON pr.player_id = p.id
            WHERE pr.rundle_id = ?
            ORDER BY pr.final_rank
            """,
            (rundle["season_id"], rundle["season_id"], rundle_id)
        ).fetchall()

        return {
            "rundle": dict(rundle),
            "standings": [dict(row) for row in standings],
        }


@router.get("/{season_number}/questions")
async def get_season_questions(
    season_number: int,
    match_day: Optional[int] = None,
    category: Optional[str] = None,
):
    """
    Get questions for a season.

    Args:
        season_number: The season number
        match_day: Optional match day filter
        category: Optional category filter
    """
    with get_connection() as conn:
        season = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?",
            (season_number,)
        ).fetchone()

        if not season:
            raise HTTPException(status_code=404, detail=f"Season {season_number} not found")

        query = """
            SELECT
                q.id,
                q.match_day,
                q.question_number,
                c.name as category,
                q.rundle_correct_pct,
                q.league_correct_pct,
                q.question_text
            FROM questions q
            JOIN categories c ON q.category_id = c.id
            WHERE q.season_id = ?
        """
        params = [season["id"]]

        if match_day:
            query += " AND q.match_day = ?"
            params.append(match_day)

        if category:
            query += " AND c.name = ?"
            params.append(category)

        query += " ORDER BY q.match_day, q.question_number"

        rows = conn.execute(query, params).fetchall()

        return {
            "season": season_number,
            "questions": [dict(row) for row in rows],
        }
