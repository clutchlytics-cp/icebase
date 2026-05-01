-- =============================================================================
-- ICEBASE — PHASE 2 | NOTEBOOK 05
-- Silver bridge_promo & quarantine_tickets — SQL
-- Idaho Mashers Hockey Analytics Platform
-- =============================================================================
-- PIPELINE SOURCE FILE — DO NOT RUN AS A STANDALONE NOTEBOOK.
-- Add to the icebase-bronze-to-silver pipeline as a source file.
--
-- LANGUAGE: SQL (pure SQL notebook)
--
-- TABLES DEFINED HERE:
--   icebase.silver.bridge_promo       (Materialized View)
--   icebase.silver.quarantine_tickets (Streaming Table)
--
-- These two tables are grouped in one notebook because:
--   - Both are pure SQL with no cross-stream union logic
--   - Neither depends on the other — they can be in the same file
--   - Keeps the pipeline source file count manageable
-- =============================================================================


-- =============================================================================
-- TABLE 1: bridge_promo (Materialized View)
-- =============================================================================
-- SOURCE:   icebase.bronze.raw_promotions (static Delta table from Phase 1)
--
-- WHY MATERIALIZED VIEW:
--   raw_promotions is a fully written historical Delta table.
--   Promo records don't change after being written. Full recompute
--   on each pipeline run gives a consistent, clean snapshot.
--
-- BUSINESS PURPOSE:
--   The promo bridge is the "bad story" engine for the Idaho Mashers.
--   During the midseason slump, marketing flooded the market with discounts.
--   This table enables the Gold layer to calculate:
--     - True promo ROI (face_value vs actual revenue)
--     - Promo density by season phase (slump spikes dramatically)
--     - Margin erosion analysis (promo_impact_score)
--
-- DERIVED COLUMNS:
--   promo_impact_score : discount_pct × face_value — revenue given away per ticket
--   discount_tier      : 'deep' (≥40%), 'moderate' (20–40%), 'light' (<20%)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW icebase.silver.bridge_promo (
  CONSTRAINT promo_id_not_null   EXPECT (promo_id IS NOT NULL),
  CONSTRAINT ticket_id_not_null  EXPECT (ticket_id IS NOT NULL),
  CONSTRAINT valid_discount      EXPECT (discount_pct BETWEEN 0.05 AND 0.60),
  CONSTRAINT discount_positive   EXPECT (discount_amount > 0)
)
COMMENT "Silver promo bridge — ticket-to-promo linkage with ROI and margin metrics"
TBLPROPERTIES (
  "quality" = "silver",
  "team"    = "idaho_mashers"
)
AS
SELECT
  promo_id,
  ticket_id,
  customer_id,
  game_id,
  promo_type,
  ROUND(discount_pct, 4)                              AS discount_pct,
  ROUND(face_value, 2)                                AS face_value,
  ROUND(discount_amount, 2)                           AS discount_amount,
  redeemed,
  season_phase,

  -- Revenue given away per ticket: higher = more damaging to margin
  ROUND(discount_pct * face_value, 2)                 AS promo_impact_score,

  -- Discount depth bucketing for segment-level promo analysis
  CASE
    WHEN discount_pct >= 0.40 THEN 'deep'
    WHEN discount_pct >= 0.20 THEN 'moderate'
    ELSE 'light'
  END                                                 AS discount_tier,

  _ingested_at,
  _source

FROM icebase.bronze.raw_promotions
WHERE redeemed = TRUE;  -- Only redeemed promos affect revenue


-- =============================================================================
-- TABLE 2: quarantine_tickets (Streaming Table)
-- =============================================================================
-- SOURCE:   Same Volume landing zone as raw_tickets_stream (notebook 01)
--           /Volumes/icebase/bronze/landing_tickets/
--
-- PURPOSE:
--   In notebook 04, fact_tickets uses expect_or_drop to hard-drop records
--   with null IDs or zero/negative prices. Those records are removed from
--   Silver but they don't automatically go anywhere — they're just gone.
--
--   This quarantine table reads the same raw source with an INVERTED filter
--   — it ONLY captures the bad records. This means:
--     1. Bad records are preserved for investigation, not silently lost
--     2. You can alert on quarantine_tickets row count spikes
--     3. You can replay records after fixing the upstream issue
--
-- BUSINESS VALUE:
--   In a real sports org, a spike in quarantine_tickets means a simulator
--   bug or upstream feed issue. This table feeds a Databricks SQL Alert
--   that fires when count > 0 (set up in Phase 3 orchestration).
--
-- NOTE: EXPECT without ON VIOLATION logs warnings for documentation purposes.
--       We WANT these bad records in the quarantine table.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE icebase.silver.quarantine_tickets (
  CONSTRAINT is_bad_record EXPECT (
    ticket_id   IS NULL OR
    customer_id IS NULL OR
    game_id     IS NULL OR
    ticket_price <= 0
  )
)
COMMENT "Quarantine — tickets that failed Silver quality gates, preserved for investigation"
TBLPROPERTIES (
  "quality" = "quarantine",
  "team"    = "idaho_mashers"
)
AS
SELECT
  ticket_id,
  customer_id,
  game_id,
  ticket_price,
  purchase_channel,
  season_phase,
  _metadata.file_name                         AS _source_filename,

  -- Reason classification for easier triage
  CASE
    WHEN ticket_id   IS NULL THEN 'null_ticket_id'
    WHEN customer_id IS NULL THEN 'null_customer_id'
    WHEN game_id     IS NULL THEN 'null_game_id'
    WHEN ticket_price <= 0  THEN 'invalid_price'
    ELSE 'unknown'
  END                                         AS quarantine_reason,

  current_timestamp()                         AS quarantine_ts,
  _metadata.file_path                         AS _source_file

FROM STREAM read_files(
  '/Volumes/icebase/bronze/landing_tickets/',
  format  => 'json',
  schema  => 'ticket_id STRING, customer_id STRING, game_id STRING, ticket_price DOUBLE, purchase_channel STRING, season_phase STRING'
)
WHERE
  ticket_id   IS NULL
  OR customer_id IS NULL
  OR game_id     IS NULL
  OR ticket_price <= 0;