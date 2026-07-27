"""
merge_contacts.py  — 100% LOCAL, nothing is uploaded or sent anywhere.

One row PER PERSON (instead of one row per number). A person's numbers, which
DataMoon splits across personal_phone / mobile_phone / direct_number /
company_phone, are collapsed into a single row with one column per type, plus a
combined `all_numbers` column.

People are identified (and merged across files) by:
  1. sha256_lc_hem  (DataMoon's hashed email) if present, else
  2. email          (lowercased), else
  3. first+last name (lowercased)

Handles all three layouts: 58-col export, 29-col audience_export, and the
uppercase xlsx (PHONE / DIRECT_PHONE / COMPANY_PHONE / EMAIL / FIRST_NAME ...).

Run:  python tools/merge_contacts.py
Output: exports/contacts_by_person.csv
"""
from __future__ import annotations

import csv
import glob
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, "DataMoon")
OUT_DIR = os.path.join(BASE, "exports")

PHONE_TYPES = ["personal_phone", "mobile_phone", "direct_number", "company_phone"]


def normalize_phone(raw: str) -> str | None:
    """US-normalize to +1XXXXXXXXXX; None if not a valid 10-digit number."""
    if not raw:
        return None
    d = re.sub(r"[^0-9]", "", str(raw))
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"+1{d}" if len(d) == 10 else None


def classify_phone(col_lower: str) -> str | None:
    """Map any phone-ish column name to one of our 4 canonical types."""
    if "mobile" in col_lower:
        return "mobile_phone"
    if "direct" in col_lower:
        return "direct_number"
    if "company" in col_lower and ("phone" in col_lower or "number" in col_lower):
        return "company_phone"
    if "phone" in col_lower or col_lower == "personal_phone":
        return "personal_phone"
    return None


def iter_rows(path: str):
    """Yield dict rows (keys are lowercased) from a CSV or XLSX file."""
    if path.lower().endswith(".csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                yield {(k or "").strip().lower(): (v or "") for k, v in row.items()}
    elif path.lower().endswith(".xlsx"):
        from openpyxl import load_workbook

        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        header = [str(c).strip().lower() if c is not None else "" for c in next(it)]
        for r in it:
            yield {header[i]: ("" if v is None else str(v)) for i, v in enumerate(r)}
        wb.close()


def get_email(row: dict) -> str:
    for col in ("personal_emails", "email", "business_email"):
        val = (row.get(col) or "").strip()
        if val:
            return val.split(";")[0].split(",")[0].strip().lower()
    return ""


def person_key(row: dict, email: str) -> str:
    hem = (row.get("sha256_lc_hem") or "").strip().lower()
    if hem:
        return "hem:" + hem
    if email:
        return "email:" + email
    name = (row.get("first_name") or "").strip().lower() + "|" + (row.get("last_name") or "").strip().lower()
    return "name:" + name if name != "|" else ""


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    files = sorted(
        glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
        + glob.glob(os.path.join(DATA_DIR, "**", "*.xlsx"), recursive=True)
    )

    people: dict[str, dict] = {}
    rows_scanned = 0
    skipped_no_identity = 0

    for path in files:
        rel = os.path.relpath(path, DATA_DIR)
        for row in iter_rows(path):
            rows_scanned += 1
            email = get_email(row)
            key = person_key(row, email)
            if not key:
                skipped_no_identity += 1
                continue

            rec = people.get(key)
            if rec is None:
                rec = {
                    "first_name": "",
                    "last_name": "",
                    "email": "",
                    "phones": {t: set() for t in PHONE_TYPES},
                    "sources": set(),
                }
                people[key] = rec

            # Fill name/email from the first record that has them.
            if not rec["first_name"]:
                rec["first_name"] = (row.get("first_name") or "").strip()
            if not rec["last_name"]:
                rec["last_name"] = (row.get("last_name") or "").strip()
            if not rec["email"] and email:
                rec["email"] = email

            # Collect every phone-ish column into its canonical bucket.
            for col, val in row.items():
                ptype = classify_phone(col)
                if not ptype:
                    continue
                num = normalize_phone(val)
                if num:
                    rec["phones"][ptype].add(num)
            rec["sources"].add(rel)

    # For each person, collapse their phones (across all types) to a DISTINCT,
    # ordered list, then spread them across numbered columns phone_1, phone_2, ...
    def distinct_numbers(rec) -> list[str]:
        nums: set[str] = set()
        for t in PHONE_TYPES:
            nums.update(rec["phones"][t])
        return sorted(nums)

    max_phones = max((len(distinct_numbers(rec)) for rec in people.values()), default=0)
    phone_cols = [f"phone_{i}" for i in range(1, max_phones + 1)]

    # Write one row per person with one column per distinct number.
    out_path = os.path.join(OUT_DIR, "contacts_by_person.csv")
    fields = ["first_name", "last_name", "email"] + phone_cols + ["num_count", "sources"]
    people_with_phone = 0
    total_distinct_numbers: set[str] = set()

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for rec in people.values():
            nums = distinct_numbers(rec)
            row_out = {
                "first_name": rec["first_name"],
                "last_name": rec["last_name"],
                "email": rec["email"],
                "num_count": len(nums),
                "sources": ";".join(sorted(rec["sources"])),
            }
            for i, num in enumerate(nums, 1):        # phone_1, phone_2, ...
                row_out[f"phone_{i}"] = num
            if nums:
                people_with_phone += 1
                total_distinct_numbers.update(nums)
            w.writerow(row_out)

    print(f"Max numbers for one person : {max_phones}  (-> phone_1..phone_{max_phones})")

    print(f"Files scanned          : {len(files)}")
    print(f"Rows scanned           : {rows_scanned}")
    print(f"Rows with no identity  : {skipped_no_identity}")
    print(f"Distinct people        : {len(people)}")
    print(f"People with >=1 number : {people_with_phone}")
    print(f"Distinct numbers total : {len(total_distinct_numbers)}")
    print(f"\nWrote: {out_path}")


if __name__ == "__main__":
    main()
