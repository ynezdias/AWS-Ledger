"""
Glue job 2 of 3 — overlap_analysis   (STEP 2 of your quality_report process)
==========================================================================
Compares the day's normalized leads against data we ALREADY own and splits
them with NO DATA LOSS, using your documented rule:

    "Match key: email OR phone; fallback name+street address"

  incoming lead matches lead_pool OR datamoon_history (by ANY of the three
  keys)                                       -> overlap_leads  (append-only)
  incoming lead matches nothing               -> refined_leads  (kept, incl.
                                                 incomplete rows)

Reconciliation mirrors your quality_report EXACTLY:
    rows_in = internal_dups + overlap + final     (must balance)
where internal_dups was recorded by STEP 1 (dedup_normalize).

Runs the joins in RDS PostgreSQL. STUB — supply the DB connection at deploy.
The match is expressed as three UNION'd joins so we can record WHICH key hit.
"""

import sys

# Insert matches. A single incoming lead can match on more than one key/source;
# we DISTINCT on incoming_lead_id when reconciling so it counts once as "overlap".
OVERLAP_SQL = """
INSERT INTO ledger.overlap_leads
    (incoming_lead_id, matched_source, matched_via, matched_ref_id, match_confidence)
-- ranked by key strength: email (1.0) > phone (0.95) > name+address (0.80)
SELECT r.lead_id, m.src, m.via, m.ref_id, m.conf
FROM ledger.raw_leads_normalized r
JOIN LATERAL (
    -- lead_pool matches
    SELECT 'lead_pool'::text AS src, 'email'::text AS via, p.lead_id AS ref_id, 1.000 AS conf
      FROM ledger.lead_pool p
     WHERE p.email_key IS NOT NULL AND p.email_key = r.email_key
    UNION ALL
    SELECT 'lead_pool', 'phone', p.lead_id, 0.950
      FROM ledger.lead_pool p
     WHERE p.phone_key IS NOT NULL AND p.phone_key = r.phone_key
    UNION ALL
    SELECT 'lead_pool', 'name_addr', p.lead_id, 0.800
      FROM ledger.lead_pool p
     WHERE p.nameaddr_key IS NOT NULL AND p.nameaddr_key = r.nameaddr_key
    UNION ALL
    -- datamoon_history matches
    SELECT 'datamoon_history', 'email', h.lead_id, 1.000
      FROM ledger.datamoon_history h
     WHERE h.email_key IS NOT NULL AND h.email_key = r.email_key
    UNION ALL
    SELECT 'datamoon_history', 'phone', h.lead_id, 0.950
      FROM ledger.datamoon_history h
     WHERE h.phone_key IS NOT NULL AND h.phone_key = r.phone_key
    UNION ALL
    SELECT 'datamoon_history', 'name_addr', h.lead_id, 0.800
      FROM ledger.datamoon_history h
     WHERE h.nameaddr_key IS NOT NULL AND h.nameaddr_key = r.nameaddr_key
) m ON true
WHERE r.source_dt = %(source_dt)s AND r.segment = %(segment)s;
"""

# Everything with NO overlap row becomes a refined lead — including incomplete
# rows (is_complete=false), which we keep per your "kept at end for future" rule.
REFINED_SQL = """
INSERT INTO ledger.refined_leads
    (incoming_lead_id, email_key, phone_key, first_name, last_name,
     personal_email, personal_phone, segment, score_category, is_complete, source_dt)
SELECT r.lead_id, r.email_key, r.phone_key, r.first_name, r.last_name,
       r.personal_email, r.personal_phone, r.segment, r.score_category,
       r.is_complete, r.source_dt
FROM ledger.raw_leads_normalized r
WHERE r.source_dt = %(source_dt)s AND r.segment = %(segment)s
  AND NOT EXISTS (
        SELECT 1 FROM ledger.overlap_leads o WHERE o.incoming_lead_id = r.lead_id
  );
"""

# Reconcile: unique-in (this batch's rows) must equal overlap + final.
# internal_dups (from STEP 1) accounts for the rest vs the original file count.
RECONCILE_SQL = """
SELECT
  (SELECT count(*) FROM ledger.raw_leads_normalized
     WHERE source_dt = %(source_dt)s AND segment = %(segment)s) AS unique_in,
  (SELECT count(DISTINCT incoming_lead_id) FROM ledger.overlap_leads
     WHERE incoming_lead_id IN
       (SELECT lead_id FROM ledger.raw_leads_normalized
          WHERE source_dt = %(source_dt)s AND segment = %(segment)s)) AS overlapped,
  (SELECT count(*) FROM ledger.refined_leads
     WHERE source_dt = %(source_dt)s AND segment = %(segment)s) AS refined;
"""


def run(source_dt: str, segment: str, conn):
    """Execute the split in one transaction and enforce the balance check."""
    params = {"source_dt": source_dt, "segment": segment}
    with conn:  # commit on success, rollback on any error
        with conn.cursor() as cur:
            cur.execute(OVERLAP_SQL, params)
            cur.execute(REFINED_SQL, params)
            cur.execute(RECONCILE_SQL, params)
            unique_in, overlapped, refined = cur.fetchone()

    if overlapped + refined != unique_in:
        raise RuntimeError(
            f"Reconcile FAILED for {segment}/{source_dt}: "
            f"{overlapped} + {refined} != {unique_in} (possible data loss!)"
        )
    print(
        f"[overlap] {segment}/{source_dt}: unique_in={unique_in} "
        f"overlap={overlapped} refined={refined}  (BALANCED)"
    )


if __name__ == "__main__":
    print("Glue-job stub. run(source_dt, segment, conn) does the reconcilable split.")
    print("Args at deploy:", sys.argv[1:])
