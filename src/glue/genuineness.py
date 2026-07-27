"""
Glue job 3 of 3 — genuineness   (STEP 3 quality layer)
==========================================================================
Scores each REFINED lead for how "genuine" it looks. Two layers:

  A) RULE-BASED (built now): our own checks PLUS DataMoon's own signals that
     already ship in the data:
       - score_category           ('low' | 'medium' | 'high')
       - email_validation_status  (DataMoon's email verification result)
       - valid email syntax / real-looking domain
       - valid phone present
       - completeness (name + address + email)

  B) ENRICHMENT (optional hook): call a third-party verification API and merge
     its confidence into enrichment_score. Isolated so it can be switched on
     later without touching layer A.

Writes results to ledger.genuineness_scores.
"""

import re

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DISPOSABLE_DOMAINS = {"mailinator.com", "tempmail.com", "guerrillamail.com"}

# DataMoon's own quality tier -> a starting confidence.
SCORE_CATEGORY_BASE = {"high": 1.00, "medium": 0.80, "low": 0.55}
# DataMoon email-validation outcomes we treat as trustworthy.
GOOD_EMAIL_STATUSES = {"valid", "verified", "deliverable", "ok"}
BAD_EMAIL_STATUSES = {"invalid", "undeliverable", "bounced"}


# ==========================================================================
# A) Rule-based scoring
# ==========================================================================
def score_rules(lead: dict) -> dict:
    """Return {'rule_score', 'reason_codes', 'verdict'} for one refined lead."""
    reasons: list[str] = []

    # Start from DataMoon's own quality tier if present, else neutral 0.8.
    category = (lead.get("score_category") or "").strip().lower()
    score = SCORE_CATEGORY_BASE.get(category, 0.80)
    if category in SCORE_CATEGORY_BASE and category != "high":
        reasons.append(f"DM_SCORE_{category.upper()}")

    email = (lead.get("personal_email") or "").strip().lower()
    phone = (lead.get("personal_phone") or "").strip()
    name = ((lead.get("first_name") or "") + (lead.get("last_name") or "")).strip()
    val_status = (lead.get("email_validation_status") or "").strip().lower()

    # DataMoon email-validation signal
    if val_status in GOOD_EMAIL_STATUSES:
        score += 0.10
    elif val_status in BAD_EMAIL_STATUSES:
        reasons.append("DM_EMAIL_INVALID")
        score -= 0.40

    # Our own email checks
    if not email:
        reasons.append("NO_EMAIL")
        score -= 0.35
    elif not EMAIL_RE.match(email):
        reasons.append("BAD_EMAIL")
        score -= 0.35
    else:
        domain = email.split("@", 1)[1]
        if domain in DISPOSABLE_DOMAINS:
            reasons.append("DISPOSABLE_EMAIL")
            score -= 0.30

    # Phone + completeness
    if not phone:
        reasons.append("NO_PHONE")
        score -= 0.25
    if not name:
        reasons.append("NO_NAME")
        score -= 0.10
    if not lead.get("is_complete", False):
        reasons.append("INCOMPLETE")     # kept, but flagged (your process)

    if not email and not phone:
        reasons.append("UNREACHABLE")
        score = 0.0

    score = max(0.0, min(1.0, score))
    verdict = "genuine" if score >= 0.7 else "suspect" if score >= 0.4 else "reject"
    return {"rule_score": round(score, 3), "reason_codes": reasons, "verdict": verdict}


# ==========================================================================
# B) Third-party enrichment (OPTIONAL — off until a vendor is wired in)
# ==========================================================================
def score_enrichment(lead: dict, enabled: bool = False) -> float | None:
    """Return a 0..1 confidence from a verification vendor, or None if disabled."""
    if not enabled:
        return None
    # TODO: call your chosen vendor (email/phone verification, business lookup).
    raise NotImplementedError("Wire in your enrichment vendor before enabling.")


def score_lead(lead: dict, enrichment_enabled: bool = False) -> dict:
    result = score_rules(lead)
    result["enrichment_score"] = score_enrichment(lead, enabled=enrichment_enabled)
    return result


# ==========================================================================
# Local self-test:  python genuineness.py
# ==========================================================================
if __name__ == "__main__":
    samples = [
        {"personal_email": "alice@example.com", "personal_phone": "+12125550101",
         "first_name": "Alice", "last_name": "A", "score_category": "high",
         "email_validation_status": "valid", "is_complete": True},
        {"personal_email": "not-an-email", "personal_phone": "",
         "first_name": "", "last_name": "", "score_category": "low",
         "email_validation_status": "invalid", "is_complete": False},
        {"personal_email": "x@mailinator.com", "personal_phone": "+12125550102",
         "first_name": "X", "last_name": "Y", "score_category": "medium",
         "email_validation_status": "", "is_complete": False},
        {"personal_email": "", "personal_phone": "",
         "first_name": "Ghost", "last_name": "", "score_category": "",
         "email_validation_status": "", "is_complete": False},
    ]
    for s in samples:
        print((s.get("personal_email") or "(none)"), "->", score_lead(s))
