-- =============================================================================
-- AWS Ledger — DataMoon Lead Pipeline : PostgreSQL schema
-- =============================================================================
-- Run once against your RDS PostgreSQL database:
--   psql "postgresql://USER:PASSWORD@HOST:5432/DBNAME" -f sql/schema.sql
--
-- This model reproduces the process documented in your DataMoon quality_report
-- files, so the AWS pipeline matches what you already do by hand:
--
--   STEP 1  normalize + internal dedup   (NO rows dropped for missing fields)
--   STEP 2  overlap vs lead pool + history:  email OR phone OR (name+street)
--   STEP 3  final = unique, non-overlapping  (incomplete rows kept, flagged)
--   RECONCILE   rows_in = internal_dup + overlap + final   (must balance)
--
-- MATCHING uses THREE independent keys (an OR match), not one combined key:
--   email_key     = sha256_lc_hem (DataMoon's hashed email) — privacy-safe
--   phone_key     = last-10 digits of personal_phone (fallback mobile_phone)
--   nameaddr_key  = sha256( lower(first|last|street|zip) )  — fallback only
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ledger;
SET search_path TO ledger, public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- -----------------------------------------------------------------------------
-- 1. raw_leads_normalized
--    A day's DataMoon feed after Glue normalizes + internally dedupes it.
--    Holds the canonical superset of both export variants (58-col full &
--    29-col audience_export). Original record preserved in `payload`.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_leads_normalized (
    lead_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- three match keys (any may be NULL; overlap is an OR across them)
    email_key       TEXT,                        -- sha256_lc_hem or sha256(email)
    phone_key       TEXT,                        -- normalized 10-digit phone
    nameaddr_key    TEXT,                        -- fallback name+street hash

    -- contact + identity (mapped from DataMoon columns)
    first_name      TEXT,
    last_name       TEXT,
    personal_email  TEXT,                        -- first of personal_emails
    business_email  TEXT,
    personal_phone  TEXT,                        -- normalized to +1XXXXXXXXXX
    mobile_phone    TEXT,
    personal_address TEXT,
    personal_city   TEXT,
    personal_state  TEXT,                        -- 2-letter
    personal_zip    TEXT,
    company_name    TEXT,
    company_domain  TEXT,

    -- DataMoon's own quality signals (feed the genuineness stage)
    score_category  TEXT,                        -- 'low' | 'medium' | 'high'
    email_validation_status TEXT,

    -- lineage + housekeeping
    segment         TEXT,                        -- B2B | B2C_Loan | Dm | QuickBusiness
    source_file     TEXT,                        -- original CSV/xlsx name
    source_dt       DATE        NOT NULL,        -- batch/partition day
    raw_s3_key      TEXT        NOT NULL,        -- pointer to immutable raw file
    is_complete     BOOLEAN     NOT NULL DEFAULT false,  -- has name+addr+email
    payload         JSONB,                       -- full original record
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_email_key    ON raw_leads_normalized (email_key);
CREATE INDEX IF NOT EXISTS idx_raw_phone_key    ON raw_leads_normalized (phone_key);
CREATE INDEX IF NOT EXISTS idx_raw_nameaddr_key ON raw_leads_normalized (nameaddr_key);
CREATE INDEX IF NOT EXISTS idx_raw_source_dt    ON raw_leads_normalized (source_dt);


-- -----------------------------------------------------------------------------
-- 2. datamoon_history
--    Every DataMoon lead we have accepted before. New feeds overlap against
--    this AND the lead pool. Partitioned by month to stay fast.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datamoon_history (
    lead_id         BIGINT      NOT NULL,
    email_key       TEXT,
    phone_key       TEXT,
    nameaddr_key    TEXT,
    first_name      TEXT,
    last_name       TEXT,
    personal_email  TEXT,
    first_seen_dt   DATE        NOT NULL,        -- partition column
    payload         JSONB,
    PRIMARY KEY (lead_id, first_seen_dt)
) PARTITION BY RANGE (first_seen_dt);

CREATE TABLE IF NOT EXISTS datamoon_history_2026_07
    PARTITION OF datamoon_history
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS datamoon_history_2026_08
    PARTITION OF datamoon_history
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE INDEX IF NOT EXISTS idx_hist_email_key    ON datamoon_history (email_key);
CREATE INDEX IF NOT EXISTS idx_hist_phone_key    ON datamoon_history (phone_key);
CREATE INDEX IF NOT EXISTS idx_hist_nameaddr_key ON datamoon_history (nameaddr_key);


-- -----------------------------------------------------------------------------
-- 3. lead_pool
--    Your EXISTING AWS lead pool (~4M emails / 5.3M phones / 2.5M name-addr keys
--    per your quality reports). Columns below are the canonical shape the
--    pipeline needs; ADJUST to match the real AWS table once we map it, or
--    populate these three key columns from it via a view/ETL.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_pool (
    lead_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email_key       TEXT,
    phone_key       TEXT,
    nameaddr_key    TEXT,
    personal_email  TEXT,
    personal_phone  TEXT,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pool_email_key    ON lead_pool (email_key);
CREATE INDEX IF NOT EXISTS idx_pool_phone_key    ON lead_pool (phone_key);
CREATE INDEX IF NOT EXISTS idx_pool_nameaddr_key ON lead_pool (nameaddr_key);


-- -----------------------------------------------------------------------------
-- 4. overlap_leads   (APPEND-ONLY — never updated or deleted)
--    An incoming lead matched something we already own. We record WHICH key
--    matched and WHERE, and keep the incoming lead untouched.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS overlap_leads (
    overlap_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incoming_lead_id BIGINT     NOT NULL,        -- -> raw_leads_normalized.lead_id
    matched_source   TEXT       NOT NULL
                     CHECK (matched_source IN ('datamoon_history', 'lead_pool')),
    matched_via      TEXT       NOT NULL         -- which key triggered the match
                     CHECK (matched_via IN ('email', 'phone', 'name_addr')),
    matched_ref_id   BIGINT,
    match_confidence NUMERIC(4,3) DEFAULT 1.000,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_overlap_incoming ON overlap_leads (incoming_lead_id);


-- -----------------------------------------------------------------------------
-- 5. refined_leads
--    Incoming leads that matched NOTHING — the fresh, non-overlapping leads.
--    Incomplete ones are kept too (is_complete = false), per your process.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS refined_leads (
    refined_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incoming_lead_id BIGINT    NOT NULL,         -- -> raw_leads_normalized.lead_id
    email_key        TEXT,
    phone_key        TEXT,
    first_name       TEXT,
    last_name        TEXT,
    personal_email   TEXT,
    personal_phone   TEXT,
    segment          TEXT,
    score_category   TEXT,
    is_complete      BOOLEAN   NOT NULL DEFAULT false,
    source_dt        DATE      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_refined_email_key ON refined_leads (email_key);
CREATE INDEX IF NOT EXISTS idx_refined_complete  ON refined_leads (is_complete);


-- -----------------------------------------------------------------------------
-- 6. genuineness_scores
--    Validates each refined lead. Rule-based now (also uses DataMoon's own
--    score_category + validation_status); enrichment_score reserved for a vendor.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS genuineness_scores (
    score_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    refined_id       BIGINT      NOT NULL,       -- -> refined_leads.refined_id
    rule_score       NUMERIC(4,3),
    enrichment_score NUMERIC(4,3),               -- nullable until a vendor is wired
    reason_codes     TEXT[],
    verdict          TEXT
                     CHECK (verdict IN ('genuine', 'suspect', 'reject')),
    scored_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scores_refined ON genuineness_scores (refined_id);
CREATE INDEX IF NOT EXISTS idx_scores_verdict ON genuineness_scores (verdict);


-- -----------------------------------------------------------------------------
-- 7. pipeline_runs   (bookkeeping + the reconciliation record)
--    One row per stage per batch. Stores the balanced-reconciliation counts so
--    every run is auditable, exactly like your quality_report files.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_dt       DATE        NOT NULL,
    segment         TEXT,
    stage           TEXT        NOT NULL,        -- 'etl' | 'overlap' | 'genuineness'
    status          TEXT        NOT NULL
                    CHECK (status IN ('started', 'succeeded', 'failed')),
    rows_in         BIGINT,
    internal_dups   BIGINT,
    overlap_count   BIGINT,
    final_count     BIGINT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_dt_stage ON pipeline_runs (source_dt, stage);
