-- =============================================================================
-- ICEBASE — PHASE 2 | NOTEBOOK 01
-- Bronze Streaming Tables — Auto Loader Ingestion
-- Idaho Mashers Hockey Analytics Platform
-- =============================================================================
-- PIPELINE SOURCE FILE — DO NOT RUN AS A STANDALONE NOTEBOOK.
-- Add to the icebase-bronze-to-silver pipeline as a source file.
--
-- LANGUAGE: SQL (pure SQL notebook — no Python cells)
--
-- TABLES DEFINED HERE:
--   icebase.bronze.raw_tickets_stream    (Streaming Table)
--   icebase.bronze.raw_customers_stream  (Streaming Table)
--
-- SOURCE:   Auto Loader watching /Volumes/icebase/bronze/landing_tickets/
--           Auto Loader watching /Volumes/icebase/bronze/landing_customers/
--
-- QUALITY:  WARN-only expectations at Bronze — preserve all raw data.
--           Silver layer (notebooks 03 & 04) enforces hard DROP gates.
--
-- PIPELINE CONFIG REQUIRED:
--   Catalog:       icebase
--   Target Schema: silver  (Bronze tables override with schema= in each def)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TABLE 1: raw_tickets_stream
-- Incrementally ingests new ticket JSON files dropped by the simulator.
-- Only processes files not yet seen — checkpoint managed by the pipeline.
-- Adds _ingested_at and _source_file metadata columns for lineage.
-- -----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE icebase.bronze.raw_tickets_stream (
  CONSTRAINT ticket_id_not_null    EXPECT (ticket_id IS NOT NULL),
  CONSTRAINT customer_id_not_null  EXPECT (customer_id IS NOT NULL),
  CONSTRAINT game_id_not_null      EXPECT (game_id IS NOT NULL),
  CONSTRAINT price_positive        EXPECT (ticket_price > 0)
)
COMMENT "Bronze streaming ingest of ticket transactions via Auto Loader"
TBLPROPERTIES (
  "quality" = "bronze",
  "team"    = "idaho_mashers"
)
AS
SELECT
  ticket_id,
  customer_id,
  game_id,
  game_date,
  section_tier,
  seat_row,
  seat_number,
  ticket_price,
  purchase_channel,
  purchase_ts,
  is_promo_ticket,
  is_jersey_night_game,
  is_lapsed_reactivation,
  season_phase,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file,
  _metadata.file_name         AS _source_filename
FROM STREAM read_files(
  '/Volumes/icebase/bronze/landing_tickets/',
  format  => 'json',
  schema  => 'ticket_id STRING, customer_id STRING, game_id STRING, game_date DATE, section_tier STRING, seat_row STRING, seat_number STRING, ticket_price DOUBLE, purchase_channel STRING, purchase_ts TIMESTAMP, is_promo_ticket BOOLEAN, is_jersey_night_game BOOLEAN, is_lapsed_reactivation BOOLEAN, season_phase STRING'
);

-- -----------------------------------------------------------------------------
-- TABLE 2: raw_customers_stream
-- Incrementally ingests new customer signup JSON files from the simulator.
-- NOTE: The 5,000 seed fans from Phase 1 live in icebase.bronze.raw_customers
--       (a static Delta table). This stream captures NET NEW signups only.
--       Silver dim_customer (notebook 03) unions both sources.
-- -----------------------------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE icebase.bronze.raw_customers_stream (
  CONSTRAINT customer_id_not_null  EXPECT (customer_id IS NOT NULL),
  CONSTRAINT email_not_null        EXPECT (email IS NOT NULL),
  CONSTRAINT valid_state           EXPECT (state = 'ID')
)
COMMENT "Bronze streaming ingest of new customer signups via Auto Loader"
TBLPROPERTIES (
  "quality" = "bronze",
  "team"    = "idaho_mashers"
)
AS
SELECT
  customer_id,
  full_name,
  email,
  phone,
  zip_code,
  state,
  acquisition_channel,
  initial_segment,
  is_season_holder,
  signup_date,
  is_jersey_night_acq,
  is_deeply_lapsed,
  current_timestamp()         AS _ingested_at,
  _metadata.file_path         AS _source_file,
  _metadata.file_name         AS _source_filename
FROM STREAM read_files(
  '/Volumes/icebase/bronze/landing_customers/',
  format  => 'json',
  schema  => 'customer_id STRING, full_name STRING, email STRING, phone STRING, zip_code STRING, state STRING, acquisition_channel STRING, initial_segment STRING, is_season_holder BOOLEAN, signup_date DATE, is_jersey_night_acq BOOLEAN, is_deeply_lapsed BOOLEAN'
);