"""HTML page routes (server-rendered templates)."""

from typing import Optional
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from ...config import Config, LL_CATEGORIES
from ...database import get_connection
from ...cache import response_cache
from ...metrics import MetricRegistry, Scope

# Template directory
BASE_DIR = Path(__file__).parent.parent.parent
TEMPLATES_DIR = BASE_DIR / "static" / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR) if TEMPLATES_DIR.exists() else None

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request, rundle: Optional[int] = Query(None)):
    """Homepage - Rundle standings."""
    if not templates:
        return RedirectResponse("/docs")

    with get_connection() as conn:
        season = conn.execute(
            "SELECT * FROM seasons ORDER BY season_number DESC LIMIT 1"
        ).fetchone()

        if not season:
            return templates.TemplateResponse("home.html", {
                "request": request,
                "season": None,
                "rundles": [],
                "standings": [],
                "metrics": [],
            })

        rundles = conn.execute(
            "SELECT * FROM rundles WHERE season_id = ? ORDER BY level, name",
            (season["id"],)
        ).fetchall()

        current_rundle = None
        if rundle:
            for r in rundles:
                if r["id"] == rundle:
                    current_rundle = r
                    break

        if not current_rundle:
            for r in rundles:
                if r["name"] == Config.DEFAULT_RUNDLE:
                    current_rundle = r
                    break
        if not current_rundle and rundles:
            current_rundle = rundles[0]

        standings = []
        if current_rundle:
            standings = conn.execute("""
                SELECT
                    p.ll_username,
                    pr.final_rank,
                    (SELECT COALESCE(SUM(CASE WHEN m.player1_id = p.id THEN m.player1_tca ELSE m.player2_tca END), 0)
                     FROM matches m
                     WHERE m.season_id = ? AND (m.player1_id = p.id OR m.player2_id = p.id)) as tca,
                    (SELECT COUNT(*) * 6
                     FROM matches m
                     WHERE m.season_id = ? AND (m.player1_id = p.id OR m.player2_id = p.id)) as total_q
                FROM players p
                JOIN player_rundles pr ON p.id = pr.player_id
                WHERE pr.rundle_id = ?
                ORDER BY pr.final_rank
            """, (season["id"], season["id"], current_rundle["id"])).fetchall()

        standings_list = []
        for s in standings:
            d = dict(s)
            d["ca_pct"] = round(d["tca"] / d["total_q"] * 100, 1) if d.get("total_q") else None
            standings_list.append(d)

        return templates.TemplateResponse("home.html", {
            "request": request,
            "season": dict(season),
            "rundles": [dict(r) for r in rundles],
            "current_rundle": dict(current_rundle) if current_rundle else None,
            "standings": standings_list,
            "metrics": MetricRegistry.all_info(),
        })


@router.get("/player/{username}", response_class=HTMLResponse)
async def player_profile(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player profile page with metrics."""
    if not templates:
        return RedirectResponse(f"/api/players/{username}")

    with get_connection() as conn:
        player = conn.execute(
            "SELECT * FROM players WHERE ll_username = ?",
            (username,)
        ).fetchone()

        if not player:
            return templates.TemplateResponse("player.html", {
                "request": request,
                "player": None,
                "error": f"Player '{username}' not found",
            })

        if season:
            season_row = conn.execute(
                "SELECT * FROM seasons WHERE season_number = ?",
                (season,)
            ).fetchone()
        else:
            season_row = conn.execute(
                "SELECT * FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()

        season_category_stats = []
        if season_row:
            season_category_stats = conn.execute("""
                SELECT c.name, pcs.correct_pct, pcs.total_questions
                FROM player_category_stats pcs
                JOIN categories c ON pcs.category_id = c.id
                WHERE pcs.player_id = ? AND pcs.season_id = ?
                ORDER BY pcs.correct_pct DESC
            """, (player["id"], season_row["id"])).fetchall()

        lifetime_category_stats = conn.execute("""
            SELECT c.name, pls.correct_pct, pls.total_questions
            FROM player_lifetime_stats pls
            JOIN categories c ON pls.category_id = c.id
            WHERE pls.player_id = ?
            ORDER BY pls.correct_pct DESC
        """, (player["id"],)).fetchall()

        # For backward compat: category_stats = lifetime if available, else season
        category_stats = lifetime_category_stats or season_category_stats

        match_results_raw = []
        if season_row:
            match_results_raw = conn.execute("""
                SELECT
                    q.match_day,
                    COUNT(*) as questions,
                    SUM(CASE WHEN a.correct THEN 1 ELSE 0 END) as correct
                FROM answers a
                JOIN questions q ON a.question_id = q.id
                WHERE a.player_id = ? AND q.season_id = ?
                GROUP BY q.match_day
                ORDER BY q.match_day
            """, (player["id"], season_row["id"])).fetchall()

        match_results = []
        for m in match_results_raw:
            d = dict(m)
            d["pct"] = round(d["correct"] / d["questions"] * 100, 0) if d.get("questions") else None
            match_results.append(d)

        totals = conn.execute("""
            SELECT
                COUNT(*) as total_q,
                SUM(CASE WHEN a.correct THEN 1 ELSE 0 END) as tca
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE a.player_id = ? AND q.season_id = ?
        """, (player["id"], season_row["id"])).fetchone() if season_row else None

        if not totals or totals["total_q"] == 0:
            match_totals = conn.execute("""
                SELECT
                    COUNT(*) * 6 as total_q,
                    SUM(CASE WHEN player1_id = ? THEN player1_tca ELSE player2_tca END) as tca
                FROM matches
                WHERE season_id = ? AND (player1_id = ? OR player2_id = ?)
            """, (player["id"], season_row["id"], player["id"], player["id"])).fetchone() if season_row else None
            if match_totals and match_totals["tca"]:
                totals = match_totals

        h2h_matches = []
        if season_row:
            h2h_matches = conn.execute("""
                SELECT
                    m.match_day,
                    CASE WHEN m.player1_id = ? THEN m.player1_score ELSE m.player2_score END as my_score,
                    CASE WHEN m.player1_id = ? THEN m.player2_score ELSE m.player1_score END as opp_score,
                    CASE WHEN m.player1_id = ? THEN m.player1_tca ELSE m.player2_tca END as my_tca,
                    CASE WHEN m.player1_id = ? THEN p2.ll_username ELSE p1.ll_username END as opponent,
                    CASE
                        WHEN (m.player1_id = ? AND m.player1_score > m.player2_score) OR
                             (m.player2_id = ? AND m.player2_score > m.player1_score) THEN 'W'
                        WHEN m.player1_score = m.player2_score THEN 'T'
                        ELSE 'L'
                    END as result
                FROM matches m
                JOIN players p1 ON m.player1_id = p1.id
                JOIN players p2 ON m.player2_id = p2.id
                WHERE m.season_id = ? AND (m.player1_id = ? OR m.player2_id = ?)
                ORDER BY m.match_day
            """, (player["id"], player["id"], player["id"], player["id"],
                  player["id"], player["id"], season_row["id"], player["id"], player["id"])).fetchall()

        season_id_val = season_row["id"] if season_row else None
        cache_key = f"player_metrics:{player['id']}:{season_id_val}"
        metrics_data = response_cache.get(cache_key)
        if metrics_data is None:
            metrics_data = {}
            for metric_obj in MetricRegistry.all():
                if Scope.PLAYER in metric_obj.scopes:
                    try:
                        result = metric_obj.calculate(
                            conn, Scope.PLAYER,
                            player_id=player["id"],
                            season_id=season_id_val
                        )
                        metrics_data[metric_obj.id] = {
                            "name": metric_obj.name,
                            "description": metric_obj.description,
                            "result": result.to_dict() if result else None
                        }
                    except Exception as e:
                        metrics_data[metric_obj.id] = {
                            "name": metric_obj.name,
                            "description": metric_obj.description,
                            "error": str(e)
                        }
            response_cache.set(cache_key, metrics_data)

        return templates.TemplateResponse("player.html", {
            "request": request,
            "player": dict(player),
            "season": dict(season_row) if season_row else None,
            "category_stats": [dict(c) for c in category_stats],
            "season_category_stats": [dict(c) for c in season_category_stats],
            "lifetime_category_stats": [dict(c) for c in lifetime_category_stats],
            "match_results": [dict(m) for m in match_results],
            "h2h_matches": [dict(m) for m in h2h_matches],
            "totals": dict(totals) if totals else {"total_q": 0, "tca": 0},
            "metrics": metrics_data,
            "all_metrics": MetricRegistry.all_info(),
        })


@router.get("/player/{username}/surprise", response_class=HTMLResponse)
async def player_surprise(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player surprise breakdown page."""
    if not templates:
        return RedirectResponse(f"/api/metrics/surprise/questions/{username}?season={season or 107}")

    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else Config.DEFAULT_SEASON

    return templates.TemplateResponse("surprise_questions.html", {
        "request": request,
        "username": username,
        "season": season_num,
    })


@router.get("/surprise/distribution", response_class=HTMLResponse)
async def surprise_distribution_page(request: Request, season: Optional[int] = Query(None)):
    """Surprise distribution chart page."""
    if not templates:
        return RedirectResponse(f"/api/metrics/surprise/distribution?season={season or 107}")

    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else Config.DEFAULT_SEASON

    return templates.TemplateResponse("surprise_distribution.html", {
        "request": request,
        "season": season_num,
    })


@router.get("/luck/{username}", response_class=HTMLResponse)
async def luck_page(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player luck analysis page."""
    if not templates:
        return RedirectResponse(f"/api/luck/{username}?season={season or 107}")

    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else Config.DEFAULT_SEASON

        player = conn.execute(
            "SELECT p.id, r.name as rundle FROM players p "
            "JOIN player_rundles pr ON p.id = pr.player_id "
            "JOIN rundles r ON pr.rundle_id = r.id "
            "JOIN seasons s ON r.season_id = s.id "
            "WHERE p.ll_username = ? AND s.season_number = ?",
            (username, season_num)
        ).fetchone()

        rundle = player["rundle"] if player else Config.DEFAULT_RUNDLE

    return templates.TemplateResponse("luck.html", {
        "request": request,
        "username": username,
        "season": season_num,
        "rundle": rundle,
    })


@router.get("/player/{username}/heatmap", response_class=HTMLResponse)
async def player_heatmap_page(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player performance heatmap page."""
    if not templates:
        return RedirectResponse(f"/api/players/{username}/heatmap?season={season or 107}")

    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else Config.DEFAULT_SEASON

    return templates.TemplateResponse("player_heatmap.html", {
        "request": request,
        "username": username,
        "season": season_num,
    })


@router.get("/categories/heatmap", response_class=HTMLResponse)
async def category_heatmap_page(request: Request, season: Optional[int] = Query(None)):
    """Category difficulty heatmap page."""
    if not templates:
        return RedirectResponse(f"/api/categories/heatmap?season={season or 107}")

    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else Config.DEFAULT_SEASON

    return templates.TemplateResponse("category_heatmap.html", {
        "request": request,
        "season": season_num,
        "categories": LL_CATEGORIES,
    })


@router.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request, season: Optional[int] = Query(None)):
    """Cross-player comparison page."""
    if not templates:
        return RedirectResponse("/docs")

    with get_connection() as conn:
        seasons_list = conn.execute(
            "SELECT season_number FROM seasons ORDER BY season_number DESC"
        ).fetchall()

        if season:
            default_season = season
        elif seasons_list:
            default_season = seasons_list[0]["season_number"]
        else:
            default_season = Config.DEFAULT_SEASON

    return templates.TemplateResponse("compare.html", {
        "request": request,
        "seasons": [s["season_number"] for s in seasons_list],
        "default_season": default_season,
    })
