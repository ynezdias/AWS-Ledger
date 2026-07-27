"""
extract_contacts.py  — 100% LOCAL, nothing is uploaded or sent anywhere.

Walks every CSV/XLSX under DataMoon/ and pulls out all phone / contact numbers
(personal_phone, mobile_phone, direct_number, company_phone). Normalizes US
numbers to +1XXXXXXXXXX, dedupes, and writes:

  exports/contact_numbers.csv        one row per (number, type) with name+email+source
  exports/contact_numbers_unique.txt just the distinct numbers, one per line

Run:  python tools/extract_contacts.py
"""
from __future__ import annotations

import csv
import glob
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, "DataMoon")
OUT_DIR = os.path.join(BASE, "exports")

# Columns we treat as phone/contact numbers (any column whose name matches).
PHONE_COL_RE = re.compile(r"(phone|direct_number)", re.IGNORECASE)


def normalize_phone(raw: str) -> str | None:
    """US-normalize to +1XXXXXXXXXX; return None if not a valid 10-digit number."""
    if not raw:
        return None
    d = re.sub(r"[^0-9]", "", raw)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"+1{d}" if len(d) == 10 else None


def iter_rows(path: str):
    """Yield dict rows from a CSV or XLSX file, whatever the layout."""
    if path.lower().endswith(".csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            yield from csv.DictReader(fh)
    elif path.lower().endswith(".xlsx"):
        from openpyxl import load_workbook

        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        header = [str(c) if c is not None else "" for c in next(rows)]
        for r in rows:
            yield {header[i]: ("" if v is None else str(v)) for i, v in enumerate(r)}
        wb.close()


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    files = sorted(
        glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
        + glob.glob(os.path.join(DATA_DIR, "**", "*.xlsx"), recursive=True)
    )

    rows_out: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()   # (number, type) dedup
    unique_numbers: set[str] = set()
    raw_scanned = 0

    for path in files:
        rel = os.path.relpath(path, DATA_DIR)
        for row in iter_rows(path):
            raw_scanned += 1
            phone_cols = [c for c in row.keys() if c and PHONE_COL_RE.search(c)]
            for col in phone_cols:
                num = normalize_phone(row.get(col, ""))
                if not num:
                    continue
                unique_numbers.add(num)
                key = (num, col)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                email = (row.get("personal_emails") or row.get("business_email") or "").split(";")[0].strip()
                rows_out.append(
                    {
                        "phone_e164": num,
                        "phone_type": col,
                        "first_name": (row.get("first_name") or "").strip(),
                        "last_name": (row.get("last_name") or "").strip(),
                        "email": email,
                        "source_file": rel,
                    }
                )

    # Write detailed CSV
    detail_path = os.path.join(OUT_DIR, "contact_numbers.csv")
    with open(detail_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["phone_e164", "phone_type", "first_name", "last_name", "email", "source_file"],
        )
        w.writeheader()
        w.writerows(sorted(rows_out, key=lambda r: r["phone_e164"]))

    # Write plain unique-numbers list
    uniq_path = os.path.join(OUT_DIR, "contact_numbers_unique.txt")
    with open(uniq_path, "w", encoding="utf-8") as fh:
        for n in sorted(unique_numbers):
            fh.write(n + "\n")

    # Per-type + per-file summary
    by_type: dict[str, int] = {}
    for r in rows_out:
        by_type[r["phone_type"]] = by_type.get(r["phone_type"], 0) + 1

    print(f"Files scanned      : {len(files)}")
    print(f"Rows scanned       : {raw_scanned}")
    print(f"Distinct numbers   : {len(unique_numbers)}")
    print(f"(number,type) rows : {len(rows_out)}")
    print("By phone column    :")
    for t, c in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"    {t:16} {c}")
    print(f"\nWrote: {detail_path}")
    print(f"Wrote: {uniq_path}")


if __name__ == "__main__":
    main()
