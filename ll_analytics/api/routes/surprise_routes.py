"""Additional surprise metric routes for detailed analysis."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from ...database import get_connection
from ...metrics.surprise import calculate_expected_probability, calculate_surprise

router = APIRouter()


LEVERAGE_START_DAY = 12  # Only classify leverage after this day


@router.get("/surprise/distribution")
async def surprise_distribution(
    season: int = Query(..., description="Season number"),
    rundle: Optional[str] = Query(None, description="Rundle name to filter by"),
):
    """
    Get surprise distribution over time (average surprise by match day).

    Split by player leverage (only after day 12 when standings stabilize):
    - high: Players near promotion/relegation (top/bottom 20%)
    - low: Players safely mid-table (middle 60%)
    """
    with get_connection() as conn:
        season_row = conn.execute(
            "SELECT id FROM seasons WHERE season_number = ?", (season,)
        ).fetchone()

        if not season_row:
            raise HTTPException(status_code=404, detail=f"Season {season} not found")

        season_id = season_row["id"]

        # Get rundle if specified
        rundle_filter = ""
        params = [season_id]

        if rundle:
            rundle_row = conn.execute(
                "SELECT id FROM rundles WHERE name = ? AND season_id = ?",
                (rundle, season_id)
            ).fetchone()
            if rundle_row:
                rundle_filter = "AND pr.rundle_id = ?"
                params.append(rundle_row["id"])

        # Get players with their ranks
        players = conn.execute(f"""
            SELECT p.id, p.ll_username, pr.final_rank,
                   (SELECT COUNT(*) FROM player_rundles pr2 WHERE pr2.rundle_id = pr.rundle_id) as rundle_size
            FROM players p
            JOIN player_rundles pr ON p.id = pr.player_id
            JOIN rundles r ON pr.rundle_id = r.id
            WHERE r.season_id = ? {rundle_filter}
        """, params).fetchall()

        # Classify leverage based on rank
        player_leverage = {}
        for p in players:
            rank = p["final_rank"] or 999
            size = p["rundle_size"] or 38
            pct = rank / size

            if pct <= 0.2 or pct >= 0.8:  # Top/bottom 20%
                player_leverage[p["id"]] = "high"
            else:
                player_leverage[p["id"]] = "low"

        # Calculate surprise by day for each player
        daily_surprises = {}  # day -> {"all": [], "high": [], "low": []}

        for p in players:
            player_id = p["id"]
            plev = player_leverage.get(player_id, "low")

            # Get answers for this player - use lifetime stats for category %
            answers = conn.execute("""
                SELECT
                    a.correct,
                    q.match_day,
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
                ORDER BY q.match_day
            """, (player_id, season_id)).fetchall()

            # Calculate surprise for each answer
            for row in answers:
                day = row["match_day"]
                player_cat_pct = row["player_category_pct"] or 0.5
                question_difficulty = row["rundle_correct_pct"] or 0.5

                expected = calculate_expected_probability(player_cat_pct, question_difficulty)
                surprise = calculate_surprise(row["correct"], expected)

                if day not in daily_surprises:
                    daily_surprises[day] = {"all": [], "high": [], "low": []}

                daily_surprises[day]["all"].append(surprise)
                # Only split by leverage after standings stabilize
                if day >= LEVERAGE_START_DAY:
                    daily_surprises[day][plev].append(surprise)

        # Calculate averages
        result = []
        for day in sorted(daily_surprises.keys()):
            data = daily_surprises[day]
            entry = {
                "match_day": day,
                "avg_surprise_all": round(sum(data["all"]) / len(data["all"]), 4) if data["all"] else 0,
                "count_all": len(data["all"]),
            }

            if data["high"]:
                entry["avg_surprise_high"] = round(sum(data["high"]) / len(data["high"]), 4)
                entry["count_high"] = len(data["high"])

            if data["low"]:
                entry["avg_surprise_low"] = round(sum(data["low"]) / len(data["low"]), 4)
                entry["count_low"] = len(data["low"])

            result.append(entry)

        return {
            "season": season,
            "rundle": rundle,
            "distribution": result,
            "leverage_start_day": LEVERAGE_START_DAY,
            "leverage_explanation": {
                "high": "Players in top/bottom 20% of standings (promotion/relegation zone)",
                "low": "Players in middle 60% of standings (safely mid-table)",
                "note": f"Leverage split only applies after day {LEVERAGE_START_DAY} when standings stabilize"
            }
        }


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

        # Get all answers with question details - use lifetime stats for category %
        answers = conn.execute("""
            SELECT
                a.correct,
                q.match_day,
                q.question_number,
                q.question_text,
                q.correct_answer,
                q.rundle_correct_pct,
                c.name as category,
                COALESCE(pcs.correct_pct, pls.correct_pct) as player_category_pct
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            JOIN categories c ON q.category_id = c.id
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
            ORDER BY q.match_day, q.question_number
        """, (player["id"], season_row["id"])).fetchall()

        questions = []
        total_surprise = 0

        for row in answers:
            player_cat_pct = row["player_category_pct"] or 0.5
            question_difficulty = row["rundle_correct_pct"] or 0.5

            expected = calculate_expected_probability(player_cat_pct, question_difficulty)
            surprise = calculate_surprise(row["correct"], expected)
            total_surprise += surprise

            questions.append({
                "match_day": row["match_day"],
                "question_number": row["question_number"],
                "category": row["category"],
                "question_text": row["question_text"] or "",
                "correct_answer": row["correct_answer"] or "",
                "got_correct": bool(row["correct"]),
                "expected_prob": round(expected, 3),
                "surprise": round(surprise, 3),
                "difficulty": round(question_difficulty, 3),
                "player_cat_pct": round(player_cat_pct, 3),
            })

        # Sort
        reverse = order.lower() == "desc"
        if sort_by == "surprise":
            questions.sort(key=lambda x: x["surprise"], reverse=reverse)
        elif sort_by == "match_day":
            questions.sort(key=lambda x: (x["match_day"], x["question_number"]), reverse=reverse)
        elif sort_by == "category":
            questions.sort(key=lambda x: x["category"], reverse=reverse)
        elif sort_by == "expected_prob":
            questions.sort(key=lambda x: x["expected_prob"], reverse=reverse)

        return {
            "player": username,
            "season": season,
            "total_surprise": round(total_surprise, 3),
            "avg_surprise": round(total_surprise / len(questions), 4) if questions else 0,
            "question_count": len(questions),
            "questions": questions,
        }
