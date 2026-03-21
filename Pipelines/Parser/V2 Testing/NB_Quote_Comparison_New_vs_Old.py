# Databricks notebook source
# MAGIC %md
# MAGIC # RUNZ Parser — New vs Old Quote Comparison
# MAGIC
# MAGIC Field-level validation of parsed quote data between:
# MAGIC - **New (Lambda)**: `raw_us_corporates_dev.pretrade_runz_quote_norm`
# MAGIC - **Old (Core)**: `raw_us_corporates.pretrade_runz_quote_norm`
# MAGIC
# MAGIC Emails are matched via `rfc822msgid` from the respective parser results tables.
# MAGIC Within each email, quotes are matched by `(tkr, cpn, mty, side)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

# Date range to compare — adjust as needed
DATE_FROM = "2026-01-01"
DATE_TO   = "2026-12-31"

# Max rows per sender in the mismatch detail section (keep output manageable)
MAX_DETAIL_ROWS = 200

# Numeric tolerance for price/spread/yield comparison (in original units)
PRICE_TOL  = 0.005   # ~half a cent
SPREAD_TOL = 0.5     # 0.5 bps
YIELD_TOL  = 0.001   # 0.1 bps

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Parser Results (join key: rfc822msgid → email_id)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Parser results: new Lambda run
new_results = (
    spark.table("raw_us_corporates_dev.runz_parser_results")
    .filter(F.col("status").isin("SUCCESS", "PARTIAL_PARSE"))
    .filter(F.col("email_timestamp").between(DATE_FROM, DATE_TO))
    .select(
        F.col("email_id").alias("new_email_id"),
        F.col("rfc822msgid"),
        F.col("from_email"),
        F.col("subject"),
        F.col("status").alias("new_parse_status"),
        F.col("quote_count").alias("new_quote_count"),
        F.col("header_source").alias("new_header_source"),
        F.col("parsed_header").alias("new_parsed_header"),
    )
)

# Parser results: old Core parser — UNION HTML-table and JPM space-delimited tables
# Both tables share the same schema; JPM uses email_timestamp (string) while
# the HTML table additionally has a convertedTimestamp (datetime) column.
# We parse email_timestamp for both so the date filter is consistent.
_old_html = (
    spark.table("raw_us_corporates.runz_parser_results")
    .filter(F.col("status").isin("SUCCESS", "PARTIAL_PARSE"))
    .filter(
        F.to_timestamp(F.col("email_timestamp"), "d MMM yyyy HH:mm:ss Z")
        .between(DATE_FROM, DATE_TO)
    )
    .select(
        F.col("email_id").alias("old_email_id"),
        F.col("rfc822msgid"),
        F.col("status").alias("old_parse_status"),
        F.get_json_object(F.col("message"), "$.quoteCount").cast("int").alias("old_quote_count"),
        F.lit("html").alias("old_source"),
    )
)

_old_jpm = (
    spark.table("raw_us_corporates.sp_runz_parser_results")
    .filter(F.col("status").isin("SUCCESS", "PARTIAL_PARSE"))
    .filter(
        F.to_timestamp(F.col("email_timestamp"), "d MMM yyyy HH:mm:ss Z")
        .between(DATE_FROM, DATE_TO)
    )
    .select(
        F.col("email_id").alias("old_email_id"),
        F.col("rfc822msgid"),
        F.col("status").alias("old_parse_status"),
        F.get_json_object(F.col("message"), "$.quoteCount").cast("int").alias("old_quote_count"),
        F.lit("jpm").alias("old_source"),
    )
)

old_results = _old_html.unionByName(_old_jpm)

# Join on rfc822msgid — inner join keeps only emails processed by both parsers
email_pairs = (
    new_results.join(old_results, on="rfc822msgid", how="inner")
)

print(f"Matched emails (both parsers): {email_pairs.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Quote Data

# COMMAND ----------

# New parser quotes
new_quotes_raw = spark.table("raw_us_corporates_dev.pretrade_runz_quote_norm")

# Old parser quotes
old_quotes_raw = spark.table("raw_us_corporates.pretrade_runz_quote_norm")

# COMMAND ----------

# Normalize new quotes
def _dbl(col_name):
    """try_cast to double — returns NULL instead of throwing on bad values like 'Float'."""
    return F.expr(f"try_cast(`{col_name}` as double)")

new_quotes = (
    new_quotes_raw
    .select(
        F.col("email_id").alias("new_email_id"),
        F.col("side"),
        F.col("tkr"),
        _dbl("cpn").alias("cpn"),
        F.col("mty"),
        _dbl("price").alias("new_price"),
        _dbl("spread").alias("new_spread"),
        _dbl("yield").alias("new_yield"),
        _dbl("quantity").alias("new_qty"),
        F.col("cusip").alias("new_cusip"),
        F.col("isin").alias("new_isin"),
    )
    .filter(F.col("tkr").isNotNull() | F.col("cusip").isNotNull())
)

# Normalize old quotes — convert Unix ms timestamp (not needed for field compare)
old_quotes = (
    old_quotes_raw
    .select(
        F.col("email_id").alias("old_email_id"),
        F.col("side"),
        F.col("tkr"),
        _dbl("cpn").alias("cpn"),
        F.col("mty"),
        _dbl("price").alias("old_price"),
        _dbl("spread").alias("old_spread"),
        _dbl("yield").alias("old_yield"),     # expected null in old
        _dbl("quantity").alias("old_qty"),
        F.col("cusip").alias("old_cusip"),
        F.col("isin").alias("old_isin"),
    )
    .filter(F.col("tkr").isNotNull() | F.col("cusip").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Attach Email Metadata and Scope to Matched Emails

# COMMAND ----------

email_ids_new = email_pairs.select("new_email_id").distinct()
email_ids_old = email_pairs.select("old_email_id").distinct()

new_q = new_quotes.join(email_ids_new, on="new_email_id", how="inner")
old_q = old_quotes.join(email_ids_old, on="old_email_id", how="inner")

# Bring in from_email / subject / old_source for grouping
new_q = new_q.join(
    email_pairs.select("new_email_id", "from_email", "subject", "rfc822msgid", "old_source"),
    on="new_email_id", how="left"
)
old_q = old_q.join(
    email_pairs.select("old_email_id", "rfc822msgid"),
    on="old_email_id", how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Match Quotes Within Each Email
# MAGIC
# MAGIC Match key: `(rfc822msgid, side, tkr, cpn, mty)`

# COMMAND ----------

# Standardize match key fields — all as strings to avoid implicit numeric casts in join
def std_key(df, email_id_col):
    return (
        df
        .withColumn("mty_std",  F.regexp_replace(F.lower(F.trim(F.col("mty"))), r"[^0-9/]", ""))
        .withColumn("tkr_std",  F.lower(F.trim(F.col("tkr"))))
        .withColumn("side_std", F.lower(F.trim(F.col("side"))))
        # Format cpn as string rounded to 4dp so "5.5" == "5.5000" both become "5.5000"
        # Non-numeric cpn values (bad data) become NULL and won't match — safe to drop
        .withColumn("cpn_std",  F.format_string("%.4f", F.expr("try_cast(`cpn` as double)")))
    )

new_keyed = std_key(new_q, "new_email_id")
old_keyed = std_key(old_q, "old_email_id")

# Add row_number within each match key group so that when the same bond
# appears multiple times in one email (two lots at different prices), each
# new row is paired with exactly one old row instead of cross-matching.
_match_key = ["rfc822msgid", "side_std", "tkr_std", "cpn_std", "mty_std"]
_w_new = Window.partitionBy(*_match_key).orderBy("new_price", "new_spread")
_w_old = Window.partitionBy(*_match_key).orderBy("old_price", "old_spread")

new_keyed = new_keyed.withColumn("_rn", F.row_number().over(_w_new))
old_keyed = old_keyed.withColumn("_rn", F.row_number().over(_w_old))

# Full outer join — include _rn so duplicate bond rows join 1:1
matched = (
    new_keyed
    .join(
        old_keyed.select(
            "rfc822msgid", "side_std", "tkr_std", "cpn_std", "mty_std", "_rn",
            "old_price", "old_spread", "old_yield", "old_qty",
            "old_cusip", "old_isin",
        ),
        on=_match_key + ["_rn"],
        how="full_outer",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Classify Each Matched Row

# COMMAND ----------

matched = matched.withColumn(
    "match_class",
    F.when(F.col("new_email_id").isNull(), "old_only")
     .when(F.col("old_price").isNull() & F.col("old_spread").isNull() & F.col("old_qty").isNull(), "new_only")
     .otherwise("both")
)

# For rows present in both, check field agreement
def near_equal(col_a, col_b, tol):
    """True when both non-null and within tolerance, or both null."""
    return (
        (F.col(col_a).isNull() & F.col(col_b).isNull()) |
        (F.abs(F.col(col_a) - F.col(col_b)) <= tol)
    )

matched = (
    matched
    .withColumn("price_match",  near_equal("new_price",  "old_price",  PRICE_TOL))
    .withColumn("spread_match", near_equal("new_spread", "old_spread", SPREAD_TOL))
    .withColumn("qty_match",    near_equal("new_qty",    "old_qty",    1.0))  # sizes in MM
    # yield: old is always null — flag when new has a value (informational, not a mismatch)
    .withColumn("yield_new_only", F.col("new_yield").isNotNull() & F.col("old_yield").isNull())
    .withColumn(
        "any_field_mismatch",
        (F.col("match_class") == "both") & (
            ~F.col("price_match") | ~F.col("spread_match") | ~F.col("qty_match")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary by Email

# COMMAND ----------

summary = (
    matched
    .groupBy("rfc822msgid", "from_email", "subject", "old_source")
    .agg(
        F.count(F.when(F.col("match_class") == "both",          1)).alias("matched_quotes"),
        F.count(F.when(F.col("match_class") == "new_only",      1)).alias("new_only_quotes"),
        F.count(F.when(F.col("match_class") == "old_only",      1)).alias("old_only_quotes"),
        F.count(F.when(F.col("any_field_mismatch"),             1)).alias("field_mismatches"),
        F.count(F.when(F.col("price_match")  == False,          1)).alias("price_mismatches"),
        F.count(F.when(F.col("spread_match") == False,          1)).alias("spread_mismatches"),
        F.count(F.when(F.col("qty_match")    == False,          1)).alias("qty_mismatches"),
        F.count(F.when(F.col("yield_new_only"),                 1)).alias("yield_added_by_new"),
    )
    .withColumn("total_quotes",    F.col("matched_quotes") + F.col("new_only_quotes") + F.col("old_only_quotes"))
    .withColumn("pct_field_ok",
        F.round(F.col("matched_quotes") / F.col("total_quotes") * 100, 1)
    )
    .orderBy(F.col("field_mismatches").desc(), F.col("new_only_quotes").desc())
)

display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Aggregate by Sender

# COMMAND ----------

by_sender = (
    summary
    .groupBy("from_email")
    .agg(
        F.count("rfc822msgid").alias("emails"),
        F.sum("matched_quotes").alias("matched_quotes"),
        F.sum("new_only_quotes").alias("new_only_quotes"),
        F.sum("old_only_quotes").alias("old_only_quotes"),
        F.sum("field_mismatches").alias("field_mismatches"),
        F.sum("price_mismatches").alias("price_mismatches"),
        F.sum("spread_mismatches").alias("spread_mismatches"),
        F.sum("qty_mismatches").alias("qty_mismatches"),
        F.sum("yield_added_by_new").alias("yield_added_by_new"),
    )
    .orderBy(F.col("field_mismatches").desc())
)

display(by_sender)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Overall Totals

# COMMAND ----------

totals = matched.agg(
    F.count("*").alias("total_quote_slots"),
    F.count(F.when(F.col("match_class") == "both",     1)).alias("in_both"),
    F.count(F.when(F.col("match_class") == "new_only", 1)).alias("new_only"),
    F.count(F.when(F.col("match_class") == "old_only", 1)).alias("old_only"),
    F.count(F.when(F.col("any_field_mismatch"),         1)).alias("field_mismatches"),
    F.count(F.when(F.col("price_match")  == False,      1)).alias("price_mismatches"),
    F.count(F.when(F.col("spread_match") == False,      1)).alias("spread_mismatches"),
    F.count(F.when(F.col("qty_match")    == False,      1)).alias("qty_mismatches"),
    F.count(F.when(F.col("yield_new_only"),             1)).alias("yield_added_by_new"),
)

display(totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Field Mismatch Detail
# MAGIC
# MAGIC Shows individual quotes where old and new parsers disagree on price, spread, or quantity.

# COMMAND ----------

mismatch_detail = (
    matched
    .filter(F.col("any_field_mismatch"))
    .select(
        "from_email", "subject", "rfc822msgid",
        "side_std", "tkr_std", "cpn", "mty_std",
        "new_price", "old_price",
        "new_spread", "old_spread",
        "new_qty", "old_qty",
        "price_match", "spread_match", "qty_match",
    )
    .orderBy("from_email", "rfc822msgid", "side_std", "tkr_std")
    .limit(MAX_DETAIL_ROWS)
)

display(mismatch_detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. New-Only Quotes (present in new parser, missing from old)
# MAGIC
# MAGIC Could indicate old parser was dropping rows, or new is creating phantom rows.

# COMMAND ----------

new_only_detail = (
    matched
    .filter(F.col("match_class") == "new_only")
    .select(
        "from_email", "subject", "rfc822msgid",
        "side_std", "tkr_std", "cpn", "mty_std",
        "new_price", "new_spread", "new_yield", "new_qty",
    )
    .orderBy("from_email", "rfc822msgid")
    .limit(MAX_DETAIL_ROWS)
)

display(new_only_detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Old-Only Quotes (present in old parser, missing from new)
# MAGIC
# MAGIC Could indicate a regression in the new parser.

# COMMAND ----------

old_only_detail = (
    matched
    .filter(F.col("match_class") == "old_only")
    .select(
        "rfc822msgid",
        "side_std", "tkr_std", "cpn", "mty_std",
        "old_price", "old_spread", "old_yield", "old_qty",
    )
    .orderBy("rfc822msgid")
    .limit(MAX_DETAIL_ROWS)
)

display(old_only_detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Spot-Check: Single Email Deep Dive
# MAGIC
# MAGIC Paste a specific `rfc822msgid` to inspect all quotes for one email side-by-side.

# COMMAND ----------

# Uncomment and set to investigate a specific email
# INSPECT_MSGID = "<example@bloomberg.net>"

# single = (
#     matched
#     .filter(F.col("rfc822msgid") == INSPECT_MSGID)
#     .select(
#         "side_std", "tkr_std", "cpn", "mty_std",
#         "new_price", "old_price",
#         "new_spread", "old_spread",
#         "new_yield",
#         "new_qty", "old_qty",
#         "match_class",
#         "price_match", "spread_match", "qty_match",
#     )
#     .orderBy("side_std", "tkr_std")
# )
# display(single)
