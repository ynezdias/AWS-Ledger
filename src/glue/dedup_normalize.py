"""
Glue job 1 of 3 — dedup_normalize   (STEP 1 of your quality_report process)
==========================================================================
Reads a day's RAW files from S3, maps BOTH DataMoon export layouts (the 58-col
full export and the 29-col audience_export) into ONE canonical schema,
normalizes fields, computes the THREE match keys, and removes internal
duplicates.

Matches your documented rule exactly:
  "STEP 1 - NORMALIZE & INTERNAL DEDUP (no rows dropped for missing fields)"
  -> incomplete rows are KEPT and flagged (is_complete=false), never dropped.

Writes:
  * curated/  parquet   (all unique rows, complete + incomplete)
  * a load into RDS ledger.raw_leads_normalized
  * a count of internal duplicates -> pipeline_runs (for reconciliation)

Run as an AWS Glue (Spark) job.
"""

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_dt", "s3_bucket", "segment"])
SOURCE_DT = args["source_dt"]
BUCKET = args["s3_bucket"]
SEGMENT = args["segment"]          # e.g. B2B / B2C_Loan / Dm / QuickBusiness

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session

RAW_PATH = f"s3://{BUCKET}/raw/dt={SOURCE_DT}/"
CURATED_PATH = f"s3://{BUCKET}/curated/dt={SOURCE_DT}/"


# ==========================================================================
# Column mapping — both export layouts share these source names, so we read
# defensively (a missing column becomes NULL rather than an error).
# ==========================================================================
def col_or_null(df, name):
    return F.col(name) if name in df.columns else F.lit(None).cast("string")


def to_canonical(df):
    """Map raw DataMoon columns -> the canonical internal schema."""
    # personal_emails can hold one or several addresses; take the first.
    first_email = F.trim(F.split(col_or_null(df, "personal_emails"), r"[;, ]").getItem(0))

    return df.select(
        F.lower(F.trim(col_or_null(df, "sha256_lc_hem"))).alias("sha256_lc_hem"),
        F.trim(col_or_null(df, "first_name")).alias("first_name"),
        F.trim(col_or_null(df, "last_name")).alias("last_name"),
        F.lower(first_email).alias("personal_email"),
        F.lower(F.trim(col_or_null(df, "business_email"))).alias("business_email"),
        col_or_null(df, "personal_phone").alias("personal_phone_raw"),
        col_or_null(df, "mobile_phone").alias("mobile_phone_raw"),
        F.trim(col_or_null(df, "personal_address")).alias("personal_address"),
        F.trim(col_or_null(df, "personal_city")).alias("personal_city"),
        F.upper(F.trim(col_or_null(df, "personal_state"))).alias("personal_state"),
        F.trim(col_or_null(df, "personal_zip")).alias("personal_zip"),
        F.trim(col_or_null(df, "company_name")).alias("company_name"),
        F.trim(col_or_null(df, "company_domain")).alias("company_domain"),
        F.lower(F.trim(col_or_null(df, "score_category"))).alias("score_category"),
        col_or_null(df, "personal_emails_validation_status").alias("email_validation_status"),
    )


def digits_10(colexpr):
    """Keep digits, drop a leading US '1', return the last 10 digits (or NULL)."""
    d = F.regexp_replace(colexpr, r"[^0-9]", "")
    # Drop a leading US country code '1' from 11-digit numbers -> last 10 digits.
    d = F.when(F.length(d) == 11, F.substring(d, 2, 10)).otherwise(d)
    return F.when(F.length(d) == 10, d).otherwise(F.lit(None))


def add_keys_and_flags(df):
    """Compute the three match keys, phone normalization, and is_complete."""
    pphone = digits_10(F.col("personal_phone_raw"))
    mphone = digits_10(F.col("mobile_phone_raw"))
    phone_key = F.coalesce(pphone, mphone)                 # personal first, mobile fallback

    # email_key: prefer DataMoon's hashed email; else hash our normalized email.
    email_key = F.when(
        F.col("sha256_lc_hem").isNotNull() & (F.length("sha256_lc_hem") > 0),
        F.col("sha256_lc_hem"),
    ).otherwise(
        F.when(F.col("personal_email").isNotNull(),
               F.sha2(F.col("personal_email"), 256)).otherwise(F.lit(None))
    )

    # nameaddr_key: fallback only — used when no email/phone match is possible.
    nameaddr_src = F.concat_ws(
        "|",
        F.lower(F.coalesce(F.col("first_name"), F.lit(""))),
        F.lower(F.coalesce(F.col("last_name"), F.lit(""))),
        F.lower(F.coalesce(F.col("personal_address"), F.lit(""))),
        F.coalesce(F.col("personal_zip"), F.lit("")),
    )
    nameaddr_key = F.when(
        (F.col("personal_address").isNotNull()) & (F.col("last_name").isNotNull()),
        F.sha2(nameaddr_src, 256),
    ).otherwise(F.lit(None))

    df = (
        df.withColumn("personal_phone", F.when(pphone.isNotNull(), F.concat(F.lit("+1"), pphone)))
        .withColumn("mobile_phone", F.when(mphone.isNotNull(), F.concat(F.lit("+1"), mphone)))
        .withColumn("phone_key", phone_key)
        .withColumn("email_key", email_key)
        .withColumn("nameaddr_key", nameaddr_key)
    )

    # is_complete = has name AND address AND email (your "complete records" def).
    df = df.withColumn(
        "is_complete",
        F.col("last_name").isNotNull()
        & F.col("personal_address").isNotNull()
        & F.col("email_key").isNotNull(),
    )
    return df.drop("personal_phone_raw", "mobile_phone_raw")


def internal_dedup(df):
    """Remove duplicate rows WITHIN the batch, preferring the most complete.

    Dedup identity = the first available of (email_key, phone_key, nameaddr_key).
    Returns (unique_df, dup_count) so we can reconcile like the quality reports.
    """
    df = df.withColumn(
        "_dedup_key",
        F.coalesce(F.col("email_key"), F.col("phone_key"), F.col("nameaddr_key")),
    )
    # Rows with NO key at all are never merged away (kept individually).
    keyed = df.filter(F.col("_dedup_key").isNotNull())
    keyless = df.filter(F.col("_dedup_key").isNull())

    completeness = (
        F.col("is_complete").cast("int") * 4
        + F.col("email_key").isNotNull().cast("int") * 2
        + F.col("phone_key").isNotNull().cast("int")
    )
    w = Window.partitionBy("_dedup_key").orderBy(completeness.desc())
    unique = (
        keyed.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    dup_count = keyed.count() - unique.count()
    result = unique.unionByName(keyless).drop("_dedup_key")
    return result, dup_count


# ==========================================================================
# Main
# ==========================================================================
def main():
    raw = spark.read.json(RAW_PATH)          # drain Lambda wrote JSON Lines
    canonical = to_canonical(raw)
    keyed = add_keys_and_flags(canonical)
    unique, dup_count = internal_dedup(keyed)

    unique = (
        unique.withColumn("source_dt", F.lit(SOURCE_DT))
        .withColumn("segment", F.lit(SEGMENT))
    )

    unique.write.mode("overwrite").parquet(CURATED_PATH)

    # TODO: append to ledger.raw_leads_normalized via JDBC (RDS Glue connection),
    #       and write a pipeline_runs row: stage='etl', rows_in, internal_dups=dup_count.

    print(
        f"[dedup_normalize] segment={SEGMENT} dt={SOURCE_DT} "
        f"rows_in={raw.count()} internal_dups={dup_count} unique={unique.count()}"
    )


if __name__ == "__main__":
    main()
