-- =============================================================================
-- ICEBASE — PHASE 2 | NOTEBOOK 02
-- Silver dim_game — Materialized View
-- Idaho Mashers Hockey Analytics Platform
-- =============================================================================
-- PIPELINE SOURCE FILE — DO NOT RUN AS A STANDALONE NOTEBOOK.
-- Add to the icebase-bronze-to-silver pipeline as a source file.
--
-- LANGUAGE: SQL (pure SQL notebook)
--
-- TABLE DEFINED HERE:
--   icebase.silver.dim_game  (Materialized View)
--
-- SOURCE:   icebase.bronze.raw_events (static Delta table — written by Phase 1
--           seed generator, 82 games, never changes)
--
-- WHY MATERIALIZED VIEW (not Streaming Table):
--   raw_events is a fully written Delta table — not an append-only stream.
--   Game results are final once written. A Materialized View does a clean
--   full recompute on each pipeline run, which is exactly what we want.
--   No STREAM keyword is used here.
--
-- DERIVED COLUMNS ADDED IN SILVER:
--   result_numeric      : W=1, L=0  (enables SUM aggregations and ML features)
--   is_playoff_relevant : TRUE for late_push, jersey_night, post_jersey phases
--   wins_to_date        : running win total using window function
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW icebase.silver.dim_game (
  CONSTRAINT game_id_not_null  EXPECT (game_id IS NOT NULL),
  CONSTRAINT valid_result      EXPECT (result IN ('W', 'L')),
  CONSTRAINT fill_pct_valid    EXPECT (arena_fill_pct BETWEEN 0 AND 1)
)
COMMENT "Silver game dimension — cleaned 82-game schedule with derived performance columns"
TBLPROPERTIES (
  "quality" = "silver",
  "team"    = "idaho_mashers"
)
AS
SELECT
  game_id,
  game_number,
  CAST(game_date AS DATE)                                     AS game_date,
  opponent,
  is_home_game,
  arena,
  home_score,
  away_score,
  result,

  -- Numeric result for aggregation and ML feature engineering
  CASE WHEN result = 'W' THEN 1 ELSE 0 END                   AS result_numeric,

  attendance,
  ROUND(arena_fill_pct, 3)                                    AS arena_fill_pct,
  season_type,
  season_phase,
  is_jersey_night,
  is_star_injury_game,

  -- Playoff relevance flag — late season phases where every game matters
  CASE
    WHEN season_phase IN ('late_push', 'jersey_night', 'post_jersey') THEN TRUE
    ELSE FALSE
  END                                                         AS is_playoff_relevant,

  -- Running win total ordered by game number
  -- Useful for storytelling: "Mashers were 16-6 when the slump hit"
  SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END)
    OVER (ORDER BY game_number
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   AS wins_to_date,

  _ingested_at,
  _source

FROM icebase.bronze.raw_events
ORDER BY game_number;