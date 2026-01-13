"""Database initialization and connection management."""

import sqlite3
from pathlib import Path
from contextlib import contextmanager
from typing import Generator

from .config import Config, LL_CATEGORIES


SCHEMA = """
-- Core entities
CREATE TABLE IF NOT EXISTS seasons (
    id INTEGER PRIMARY KEY,
    season_number INTEGER UNIQUE NOT NULL,
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    ll_username TEXT UNIQUE NOT NULL,
    display_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Rundles
CREATE TABLE IF NOT EXISTS rundles (
    id INTEGER PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    league TEXT NOT NULL,
    level TEXT NOT NULL,  -- A, B, C, D, E, R
    name TEXT NOT NULL,
    UNIQUE(season_id, league, level, name)
);

CREATE TABLE IF NOT EXISTS player_rundles (
    player_id INTEGER NOT NULL REFERENCES players(id),
    rundle_id INTEGER NOT NULL REFERENCES rundles(id),
    final_rank INTEGER,
    PRIMARY KEY (player_id, rundle_id)
);

-- Historical category performance
CREATE TABLE IF NOT EXISTS player_category_stats (
    player_id INTEGER NOT NULL REFERENCES players(id),
    category_id INTEGER NOT NULL REFERENCES categories(id),
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    correct_pct REAL,
    total_questions INTEGER,
    PRIMARY KEY (player_id, category_id, season_id)
);

-- Questions
CREATE TABLE IF NOT EXISTS questions (
    id INTEGER PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    match_day INTEGER NOT NULL,
    question_number INTEGER NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    rundle_correct_pct REAL,
    league_correct_pct REAL,
    question_text TEXT,
    UNIQUE(season_id, match_day, question_number)
);

-- Player answers
CREATE TABLE IF NOT EXISTS answers (
    id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    question_id INTEGER NOT NULL REFERENCES questions(id),
    correct BOOLEAN NOT NULL,
    defense_points_assigned INTEGER,
    UNIQUE(player_id, question_id)
);

-- Matches (for head-to-head tracking)
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    match_day INTEGER NOT NULL,
    player1_id INTEGER NOT NULL REFERENCES players(id),
    player2_id INTEGER NOT NULL REFERENCES players(id),
    player1_score INTEGER,
    player2_score INTEGER,
    player1_tca INTEGER,  -- Total correct answers
    player2_tca INTEGER,
    ll_match_id INTEGER,  -- LL's internal match ID for detailed scraping
    UNIQUE(season_id, match_day, player1_id, player2_id)
);

-- Per-question match results (for defense luck analysis)
CREATE TABLE IF NOT EXISTS match_questions (
    id INTEGER PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES matches(id),
    question_num INTEGER NOT NULL,  -- 1-6
    question_id INTEGER REFERENCES questions(id),  -- Links to full question data
    player1_correct BOOLEAN NOT NULL,
    player2_correct BOOLEAN NOT NULL,
    player1_defense INTEGER NOT NULL,  -- Defense pts player2 assigned to player1
    player2_defense INTEGER NOT NULL,  -- Defense pts player1 assigned to player2
    UNIQUE(match_id, question_num)
);

-- Metric cache (optional, for expensive calculations)
CREATE TABLE IF NOT EXISTS metric_cache (
    metric_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    result TEXT NOT NULL,  -- JSON
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_id, cache_key)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_answers_player ON answers(player_id);
CREATE INDEX IF NOT EXISTS idx_answers_question ON answers(question_id);
CREATE INDEX IF NOT EXISTS idx_questions_season_day ON questions(season_id, match_day);
CREATE INDEX IF NOT EXISTS idx_player_category_stats_player ON player_category_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_matches_season_day ON matches(season_id, match_day);
CREATE INDEX IF NOT EXISTS idx_match_questions_match ON match_questions(match_id);
"""


def get_db_path() -> Path:
    """Get the database path, ensuring directory exists."""
    Config.ensure_data_dir()
    return Config.DATABASE_PATH


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """Get a database connection with row factory enabled."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Initialize the database schema and seed data."""
    with get_connection() as conn:
        conn.executescript(SCHEMA)

        # Seed categories
        for category in LL_CATEGORIES:
            conn.execute(
                "INSERT OR IGNORE INTO categories (name) VALUES (?)",
                (category,)
            )

        conn.commit()
        print(f"Database initialized at {get_db_path()}")


def get_category_id(conn: sqlite3.Connection, category_name: str) -> int | None:
    """Get category ID by name."""
    row = conn.execute(
        "SELECT id FROM categories WHERE name = ?",
        (category_name,)
    ).fetchone()
    return row["id"] if row else None


def get_or_create_player(conn: sqlite3.Connection, username: str, display_name: str | None = None) -> int:
    """Get or create a player, returning their ID."""
    conn.execute(
        "INSERT OR IGNORE INTO players (ll_username, display_name) VALUES (?, ?)",
        (username, display_name or username)
    )
    row = conn.execute(
        "SELECT id FROM players WHERE ll_username = ?",
        (username,)
    ).fetchone()
    return row["id"]


def get_or_create_season(conn: sqlite3.Connection, season_number: int) -> int:
    """Get or create a season, returning its ID."""
    conn.execute(
        "INSERT OR IGNORE INTO seasons (season_number) VALUES (?)",
        (season_number,)
    )
    row = conn.execute(
        "SELECT id FROM seasons WHERE season_number = ?",
        (season_number,)
    ).fetchone()
    return row["id"]
