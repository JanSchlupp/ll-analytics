"""Main FastAPI application."""

from typing import Optional
from fastapi import FastAPI, Request, Query
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from pathlib import Path

from ..config import Config
from ..database import get_connection, init_db
from ..metrics import MetricRegistry, Scope
from .routes import players, seasons, metrics, surprise_routes, luck_routes

# Get paths
BASE_DIR = Path(__file__).parent.parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = STATIC_DIR / "templates"

# Create FastAPI app
app = FastAPI(
    title="LL Analytics",
    description="Learned League Analytics Platform - Custom metrics and analysis",
    version="0.1.0",
)

# Mount static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Set up templates
templates = Jinja2Templates(directory=TEMPLATES_DIR) if TEMPLATES_DIR.exists() else None

# Include routers
app.include_router(players.router, prefix="/api/players", tags=["Players"])
app.include_router(seasons.router, prefix="/api/seasons", tags=["Seasons"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["Metrics"])
app.include_router(surprise_routes.router, prefix="/api/metrics", tags=["Surprise"])
app.include_router(luck_routes.router, prefix="/api", tags=["Luck"])


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()


@app.get("/health")
async def health_check():
    """Health check endpoint for Render."""
    return {"status": "healthy"}


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, rundle: Optional[int] = Query(None)):
    """Homepage - Rundle standings."""
    if not templates:
        return RedirectResponse("/docs")

    with get_connection() as conn:
        # Get most recent season
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

        # Get rundles for this season
        rundles = conn.execute(
            "SELECT * FROM rundles WHERE season_id = ? ORDER BY level, name",
            (season["id"],)
        ).fetchall()

        # Select rundle based on query param or default to C_Skyline
        current_rundle = None
        if rundle:
            for r in rundles:
                if r["id"] == rundle:
                    current_rundle = r
                    break

        if not current_rundle:
            for r in rundles:
                if r["name"] == "C_Skyline":
                    current_rundle = r
                    break
        if not current_rundle and rundles:
            current_rundle = rundles[0]

        # Get standings for selected rundle
        standings = []
        if current_rundle:
            standings = conn.execute("""
                SELECT
                    p.ll_username,
                    pr.final_rank,
                    (SELECT COUNT(*) FROM answers a
                     JOIN questions q ON a.question_id = q.id
                     WHERE a.player_id = p.id AND q.season_id = ? AND a.correct = 1) as tca,
                    (SELECT COUNT(*) FROM answers a
                     JOIN questions q ON a.question_id = q.id
                     WHERE a.player_id = p.id AND q.season_id = ?) as total_q
                FROM players p
                JOIN player_rundles pr ON p.id = pr.player_id
                WHERE pr.rundle_id = ?
                ORDER BY pr.final_rank
            """, (season["id"], season["id"], current_rundle["id"])).fetchall()

        return templates.TemplateResponse("home.html", {
            "request": request,
            "season": dict(season),
            "rundles": [dict(r) for r in rundles],
            "current_rundle": dict(current_rundle) if current_rundle else None,
            "standings": [dict(s) for s in standings],
            "metrics": MetricRegistry.all_info(),
        })


@app.get("/player/{username}", response_class=HTMLResponse)
async def player_profile(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player profile page with metrics."""
    if not templates:
        return RedirectResponse(f"/api/players/{username}")

    with get_connection() as conn:
        # Get player
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

        # Get season (most recent if not specified)
        if season:
            season_row = conn.execute(
                "SELECT * FROM seasons WHERE season_number = ?",
                (season,)
            ).fetchone()
        else:
            season_row = conn.execute(
                "SELECT * FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()

        # Get player's category stats
        category_stats = []
        if season_row:
            category_stats = conn.execute("""
                SELECT c.name, pcs.correct_pct, pcs.total_questions
                FROM player_category_stats pcs
                JOIN categories c ON pcs.category_id = c.id
                WHERE pcs.player_id = ? AND pcs.season_id = ?
                ORDER BY pcs.correct_pct DESC
            """, (player["id"], season_row["id"])).fetchall()

        # Get match day results
        match_results = []
        if season_row:
            match_results = conn.execute("""
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

        # Get TCA and total
        totals = conn.execute("""
            SELECT
                COUNT(*) as total_q,
                SUM(CASE WHEN a.correct THEN 1 ELSE 0 END) as tca
            FROM answers a
            JOIN questions q ON a.question_id = q.id
            WHERE a.player_id = ? AND q.season_id = ?
        """, (player["id"], season_row["id"])).fetchone() if season_row else None

        # Calculate metrics for this player
        metrics_data = {}
        for metric in MetricRegistry.all():
            if Scope.PLAYER in metric.scopes:
                try:
                    result = metric.calculate(
                        conn, Scope.PLAYER,
                        player_id=player["id"],
                        season_id=season_row["id"] if season_row else None
                    )
                    metrics_data[metric.id] = {
                        "name": metric.name,
                        "description": metric.description,
                        "result": result.to_dict() if result else None
                    }
                except Exception as e:
                    metrics_data[metric.id] = {
                        "name": metric.name,
                        "description": metric.description,
                        "error": str(e)
                    }

        return templates.TemplateResponse("player.html", {
            "request": request,
            "player": dict(player),
            "season": dict(season_row) if season_row else None,
            "category_stats": [dict(c) for c in category_stats],
            "match_results": [dict(m) for m in match_results],
            "totals": dict(totals) if totals else {"total_q": 0, "tca": 0},
            "metrics": metrics_data,
            "all_metrics": MetricRegistry.all_info(),
        })


@app.get("/player/{username}/surprise", response_class=HTMLResponse)
async def player_surprise(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player surprise breakdown page."""
    if not templates:
        return RedirectResponse(f"/api/metrics/surprise/questions/{username}?season={season or 107}")

    # Get season number (default to most recent)
    with get_connection() as conn:
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else 107

    return templates.TemplateResponse("surprise_questions.html", {
        "request": request,
        "username": username,
        "season": season_num,
    })


@app.get("/surprise/distribution", response_class=HTMLResponse)
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
            season_num = row["season_number"] if row else 107

    return templates.TemplateResponse("surprise_distribution.html", {
        "request": request,
        "season": season_num,
    })


@app.get("/luck/{username}", response_class=HTMLResponse)
async def luck_page(request: Request, username: str, season: Optional[int] = Query(None)):
    """Player luck analysis page."""
    if not templates:
        return RedirectResponse(f"/api/luck/{username}?season={season or 107}")

    with get_connection() as conn:
        # Get season number
        if season:
            season_num = season
        else:
            row = conn.execute(
                "SELECT season_number FROM seasons ORDER BY season_number DESC LIMIT 1"
            ).fetchone()
            season_num = row["season_number"] if row else 107

        # Get player's rundle
        player = conn.execute(
            "SELECT p.id, r.name as rundle FROM players p "
            "JOIN player_rundles pr ON p.id = pr.player_id "
            "JOIN rundles r ON pr.rundle_id = r.id "
            "JOIN seasons s ON r.season_id = s.id "
            "WHERE p.ll_username = ? AND s.season_number = ?",
            (username, season_num)
        ).fetchone()

        rundle = player["rundle"] if player else "C_Skyline"

    return templates.TemplateResponse("luck.html", {
        "request": request,
        "username": username,
        "season": season_num,
        "rundle": rundle,
    })


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


def run_server():
    """Run the development server."""
    import uvicorn
    uvicorn.run(
        "ll_analytics.api.main:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=Config.DEBUG,
    )


if __name__ == "__main__":
    run_server()
