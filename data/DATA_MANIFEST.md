# Data Manifest — ll-analytics

This file describes the data files in this directory so Claude can understand
them without loading them into context.

## ll_analytics.db (SQLite)

Learned League analytics database. Created/managed by `ll_analytics/database.py`.

### Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `seasons` | LL seasons | `season_number`, `start_date`, `end_date` |
| `players` | Player profiles | `ll_username`, `ll_id`, `display_name` |
| `categories` | Question categories (seeded from `LL_CATEGORIES` config) | `name` |
| `rundles` | League groupings per season | `season_id`, `league`, `level` (A-E, R), `name` |
| `player_rundles` | Player-to-rundle assignments | `player_id`, `rundle_id`, `final_rank` |
| `player_category_stats` | Per-season category performance | `player_id`, `category_id`, `season_id`, `correct_pct`, `total_questions` |
| `player_lifetime_stats` | All-time category performance | `player_id`, `category_id`, `correct_pct`, `total_questions` |
| `questions` | Individual questions | `season_id`, `match_day`, `question_number`, `category_id`, `question_text`, `correct_answer`, `rundle_correct_pct`, `league_correct_pct` |
| `answers` | Player answers to questions | `player_id`, `question_id`, `correct`, `defense_points_assigned` |
| `matches` | Head-to-head match results | `season_id`, `match_day`, `player1_id`, `player2_id`, scores, TCA, `ll_match_id` |
| `match_questions` | Per-question match detail | `match_id`, `question_num` (1-6), correctness, defense points per player, `category_id`, `question_ca_pct` |
| `metric_cache` | Cached expensive calculations | `metric_id`, `cache_key`, `result` (JSON) |

### Key Relationships

- `matches` -> `match_questions`: Each match has 6 question rows
- `match_questions.player1_defense`: Defense points player2 assigned TO player1 on that question
- `answers.defense_points_assigned`: How many defense points were put on this question
- `players.ll_id`: Numeric LL ID used for profile/standings scraping
- Data is scraped from LearnedLeague.com via `ll_analytics/scraper/`

### Indexes

- `answers(player_id)`, `answers(question_id)`
- `questions(season_id, match_day)`
- `player_category_stats(player_id)`, `player_lifetime_stats(player_id)`
- `matches(season_id, match_day)`, `match_questions(match_id)`

### Data Pipeline

Data is populated via a 6-part scrape pipeline (`LLScraper.scrape_full()`):
1. **Standings** — `scrape_standings_stats()` → `players`, `player_rundles`
2. **User answers** — `scrape_my_answers()` → `answers`, `questions` (text/answer)
3. **Match results** — `scrape_match_results()` → `matches`
4. **Match details** — `scrape_match_details()` → `match_questions`
5. **Player profiles** — `scrape_player_profile_by_id()` → `player_lifetime_stats`
6. **Rundle answers** — `scrape_rundle_matchday()` → `answers` (all players), `questions` (category)
