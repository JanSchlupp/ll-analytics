"""Main FastAPI application."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from ..config import Config
from ..logging import setup_logging, get_logger
from ..database import init_db

setup_logging(Config.LOG_LEVEL, Config.LOG_FILE)
from .routes import players, seasons, metrics, surprise_routes, luck_routes, pages, heatmap_routes

logger = get_logger(__name__)

# Get paths
BASE_DIR = Path(__file__).parent.parent
STATIC_DIR = BASE_DIR / "static"

# Create FastAPI app
app = FastAPI(
    title="LL Analytics",
    description="Learned League Analytics Platform - Custom metrics and analysis",
    version="1.4.0",
)

# Mount static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Include routers
app.include_router(players.router, prefix="/api/players", tags=["Players"])
app.include_router(seasons.router, prefix="/api/seasons", tags=["Seasons"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["Metrics"])
app.include_router(surprise_routes.router, prefix="/api/metrics", tags=["Surprise"])
app.include_router(luck_routes.router, prefix="/api", tags=["Luck"])
app.include_router(heatmap_routes.router, tags=["Heatmaps"])
app.include_router(pages.router, tags=["Pages"])


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()


@app.get("/health")
async def health_check():
    """Health check endpoint for Render."""
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
