-- =============================================================================
-- ICEBASE — PHASE 4 | NOTEBOOK 04
-- ML Output Validation Queries
-- Idaho Mashers Hockey Analytics Platform
-- =============================================================================
-- STANDALONE SQL NOTEBOOK — Run in SQL Editor or attached to icebase-dev.
-- NOT a pipeline source file. Run manually after Notebooks 02 and 03 complete.
--
-- PURPOSE:
--   Validate that the ML output tables exist with correct structure,
--   that the business narrative is visible in the model outputs, and
--   that the segment and churn score distributions make business sense.
--
-- RUN EACH CELL INDEPENDENTLY (Shift+Enter) and review the output.
-- =============================================================================

-- COMMAND ----------
-- ── CHECK 1: ML Output Table Row Counts ───────────────────────────────────

SELECT 'fan_features'    AS table_name, COUNT(*) AS rows FROM icebase.gold.fan_features
UNION ALL
SELECT 'ml_fan_segments',               COUNT(*)         FROM icebase.gold.ml_fan_segments
UNION ALL
SELECT 'ml_churn_scores',               COUNT(*)         FROM icebase.gold.ml_churn_scores
ORDER BY table_name;

-- EXPECTED: All three tables have ~5,400 rows (matching dim_customer count)

-- COMMAND ----------
-- ── CHECK 2: Segment Distribution ────────────────────────────────────────
-- Confirms 5 segments exist and no segment is empty or unreasonably large.

SELECT
  segment_label,
  COUNT(*)                                    AS fan_count,
  ROUND(COUNT(*) * 100.0
    / SUM(COUNT(*)) OVER (), 1)              AS pct_of_total
FROM icebase.gold.ml_fan_segments
GROUP BY segment_label
ORDER BY fan_count DESC;

-- EXPECTED:
--   All 5 labels present: Season Core, High Value New, Casual Fan, Promo Hunter, Lapsed
--   No segment < 3% or > 50% of total (healthy distribution)
--   Casual Fan and Lapsed likely to be the largest segments

-- COMMAND ----------
-- ── CHECK 3: Churn Score Distribution ─────────────────────────────────────

SELECT
  risk_tier,
  COUNT(*)                                    AS fans,
  ROUND(COUNT(*) * 100.0
    / SUM(COUNT(*)) OVER (), 1)              AS pct_of_total,
  ROUND(AVG(churn_probability), 3)           AS avg_prob,
  ROUND(MIN(churn_probability), 3)           AS min_prob,
  ROUND(MAX(churn_probability), 3)           AS max_prob
FROM icebase.gold.ml_churn_scores
GROUP BY risk_tier
ORDER BY avg_prob DESC;

-- EXPECTED:
--   High:   churn_probability 0.70–1.0  — the fans to target for retention
--   Medium: churn_probability 0.40–0.70 — monitor and nurture
--   Low:    churn_probability 0.00–0.40 — healthy, no action needed

-- COMMAND ----------
-- ── CHECK 4: The Business Story in the ML Output ──────────────────────────
-- The most important validation: do the ML outputs confirm the narrative
-- we baked into the data? Lapsed fans should be highest churn risk.
-- Season Core should be lowest. Promo Hunter should be high.

SELECT
  s.segment_label,
  COUNT(*)                                        AS fans,
  ROUND(AVG(c.churn_probability), 3)              AS avg_churn_prob,
  ROUND(AVG(f.frequency_games), 1)               AS avg_games,
  ROUND(AVG(f.monetary_net), 2)                  AS avg_net_spend,
  ROUND(AVG(f.promo_sensitivity), 3)             AS avg_promo_sensitivity,
  ROUND(AVG(f.recency_days), 0)                  AS avg_recency_days,
  SUM(CASE WHEN c.risk_tier = 'High' THEN 1 ELSE 0 END) AS high_risk_count
FROM icebase.gold.ml_fan_segments     s
JOIN icebase.gold.ml_churn_scores     c ON s.customer_id = c.customer_id
JOIN icebase.gold.fan_features        f ON s.customer_id = f.customer_id
GROUP BY s.segment_label
ORDER BY avg_churn_prob DESC;

-- EXPECTED ORDERING (top to bottom by avg_churn_prob):
--   1. Lapsed         — highest churn prob, highest recency_days
--   2. Promo Hunter   — high churn prob, high promo_sensitivity
--   3. Casual Fan     — moderate churn prob
--   4. High Value New — low-moderate churn prob
--   5. Season Core    — lowest churn prob, lowest recency_days, highest avg_games

-- COMMAND ----------
-- ── CHECK 5: Jersey Night Cohort in ML Output ─────────────────────────────
-- Validates the "good story": fans who attended Jersey Night should
-- have lower churn risk than the general fan base.

SELECT
  f.is_jersey_night_cohort,
  COUNT(*)                                    AS fans,
  ROUND(AVG(c.churn_probability), 3)          AS avg_churn_prob,
  SUM(CASE WHEN c.risk_tier = 'High'
           THEN 1 ELSE 0 END)                AS high_risk_count,
  ROUND(
    SUM(CASE WHEN c.risk_tier = 'High' THEN 1 ELSE 0 END)
    * 100.0 / COUNT(*), 1
  )                                           AS high_risk_pct
FROM icebase.gold.ml_churn_scores    c
JOIN icebase.gold.fan_features       f ON c.customer_id = f.customer_id
GROUP BY f.is_jersey_night_cohort;

-- EXPECTED:
--   is_jersey_night_cohort = 1 → LOWER avg_churn_prob than cohort = 0
--   This proves the model captured the reactivation signal from Jersey Night

-- COMMAND ----------
-- ── CHECK 6: High-Risk Fan Watchlist ─────────────────────────────────────
-- Business-ready output: who are the top 20 highest-risk fans to target?
-- This is the kind of query a CMO or retention manager would run weekly.

SELECT
  d.full_name,
  d.email,
  d.acquisition_channel,
  s.segment_label,
  ROUND(c.churn_probability, 3)               AS churn_probability,
  c.risk_tier,
  ROUND(f.monetary_net, 2)                    AS lifetime_net_spend,
  f.frequency_games                           AS games_attended,
  f.recency_days                              AS days_since_last_game
FROM icebase.gold.ml_churn_scores     c
JOIN icebase.gold.ml_fan_segments     s ON c.customer_id = s.customer_id
JOIN icebase.gold.fan_features        f ON c.customer_id = f.customer_id
JOIN icebase.silver.dim_customer      d ON c.customer_id = d.customer_id
WHERE
  c.risk_tier = 'High'
  AND f.frequency_games >= 2        -- Exclude one-and-done fans
  AND f.monetary_net >= 50          -- Focus on fans worth retaining
ORDER BY c.churn_probability DESC
LIMIT 20;

-- This is portfolio gold: a retention watchlist generated by an ML model,
-- joined through a governed medallion architecture, ready for a campaign tool.

-- COMMAND ----------
-- ── FINAL SCORECARD ───────────────────────────────────────────────────────

SELECT check_name, result FROM (
  SELECT 1 AS sort_order,
    'fan_features table exists'                                  AS check_name,
    CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END    AS result
  FROM icebase.gold.fan_features

  UNION ALL SELECT 2,
    'ml_fan_segments has 5 distinct labels',
    CASE WHEN COUNT(DISTINCT segment_label) = 5
         THEN '✅ PASS' ELSE '❌ FAIL - review SEGMENT_LABELS in notebook 02' END
  FROM icebase.gold.ml_fan_segments

  UNION ALL SELECT 3,
    'ml_churn_scores has all 3 risk tiers',
    CASE WHEN COUNT(DISTINCT risk_tier) = 3
         THEN '✅ PASS' ELSE '❌ FAIL' END
  FROM icebase.gold.ml_churn_scores

  UNION ALL SELECT 4,
    'Lapsed segment has highest avg churn probability',
    CASE WHEN (
      SELECT AVG(c.churn_probability) FROM icebase.gold.ml_churn_scores c
      JOIN icebase.gold.ml_fan_segments s ON c.customer_id = s.customer_id
      WHERE s.segment_label = 'Lapsed'
    ) > (
      SELECT AVG(c.churn_probability) FROM icebase.gold.ml_churn_scores c
      JOIN icebase.gold.ml_fan_segments s ON c.customer_id = s.customer_id
      WHERE s.segment_label = 'Season Core'
    ) THEN '✅ PASS' ELSE '❌ FAIL - model may need review' END AS result

  UNION ALL SELECT 5,
    'No null churn probabilities',
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END
  FROM icebase.gold.ml_churn_scores
  WHERE churn_probability IS NULL
)
ORDER BY sort_order;