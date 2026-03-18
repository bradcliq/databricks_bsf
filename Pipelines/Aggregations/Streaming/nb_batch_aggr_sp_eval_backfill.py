# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Backfill: S&P Eval Price Aggregation (Historical)
# MAGIC One-time backfill for `aggr_sp_eval_streaming`.
# MAGIC
# MAGIC Processes trading days in configurable batch sizes. Computes all hours 6-18 per day.
# MAGIC Safe to re-run (uses MERGE). Run on a large cluster for best performance.

# COMMAND ----------

# -- Configuration --
TARGET_TABLE = "refined_us_corporates.aggr_sp_eval_streaming"

START_DATE = "2024-01-01"
END_DATE = "2026-03-13"
BATCH_SIZE_DAYS = 10  # trading days per batch — this query is lightweight, can go bigger

# COMMAND ----------

# ============================================================================
# Get list of trading days to process
# ============================================================================

trading_days = [
    row["tradingDate"]
    for row in spark.sql(f"""
        SELECT tradingDate
        FROM trusted_us_corporates.trading_days
        WHERE tradingDate BETWEEN DATE '{START_DATE}' AND DATE '{END_DATE}'
        ORDER BY tradingDate
    """).collect()
]

print(f"Processing {len(trading_days)} trading days from {trading_days[0]} to {trading_days[-1]}")

# COMMAND ----------

# ============================================================================
# SQL: S&P Eval Aggregation (batch version — all hours 6-18 for each date)
# ============================================================================

def sql_sp_eval(batch_start, batch_end):
    return f"""
    WITH hourly_intervals AS (
        SELECT tradingDate AS eval_date, h AS eval_hour
        FROM trusted_us_corporates.trading_days
        CROSS JOIN (SELECT explode(sequence(6, 18)) AS h)
        WHERE tradingDate BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ),
    cte_eval_filtered AS (
        SELECT
            cusip,
            asOfDateTime,
            date(from_utc_timestamp(asOfDateTime, 'America/New_York')) AS eval_date,
            hour(from_utc_timestamp(asOfDateTime, 'America/New_York')) AS eval_hour,
            ROUND(COALESCE(midSpreadToBenchmark, midSpreadToWorst, 0), 2) AS eval_mid_spread,
            ROUND(COALESCE(midPrice, 0), 3) AS eval_mid_price,
            ROUND(COALESCE(askSpreadToBenchmark, askSpreadToWorst, 0), 2) AS eval_ask_spread,
            ROUND(COALESCE(askPrice, 0), 3) AS eval_ask_price,
            ROUND(COALESCE(bidSpreadToBenchmark, bidSpreadToWorst, 0), 2) AS eval_bid_spread,
            ROUND(COALESCE(bidPrice, 0), 3) AS eval_bid_price
        FROM trusted_us_corporates.cdx_evalpricepoints
        WHERE date(from_utc_timestamp(asOfDateTime, 'America/New_York'))
              BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
          AND cusip IS NOT NULL
          AND billingCode IN ('Corporate - HY', 'Corporate IG')
    ),
    cte_eval_ranked AS (
        SELECT
            hi.eval_date,
            hi.eval_hour,
            ef.cusip,
            ef.asOfDateTime,
            ef.eval_mid_spread,
            ef.eval_mid_price,
            ef.eval_ask_spread,
            ef.eval_ask_price,
            ef.eval_bid_spread,
            ef.eval_bid_price,
            ROW_NUMBER() OVER (
                PARTITION BY hi.eval_date, hi.eval_hour, ef.cusip
                ORDER BY ef.asOfDateTime DESC
            ) AS rownum
        FROM cte_eval_filtered ef
        JOIN hourly_intervals hi
            ON ef.eval_date = hi.eval_date
            AND ef.eval_hour <= hi.eval_hour
    )
    SELECT
        cusip,
        asOfDateTime,
        eval_date,
        eval_hour,
        eval_mid_spread,
        eval_mid_price,
        eval_bid_spread,
        eval_bid_price,
        eval_ask_spread,
        eval_ask_price
    FROM cte_eval_ranked
    WHERE rownum = 1
    """

# COMMAND ----------

# ============================================================================
# Main processing loop
# ============================================================================

total_batches = (len(trading_days) + BATCH_SIZE_DAYS - 1) // BATCH_SIZE_DAYS

for i in range(0, len(trading_days), BATCH_SIZE_DAYS):
    batch = trading_days[i:i + BATCH_SIZE_DAYS]
    batch_start = str(batch[0])
    batch_end = str(batch[-1])
    batch_num = (i // BATCH_SIZE_DAYS) + 1

    print(f"Batch {batch_num}/{total_batches}: {batch_start} to {batch_end} ({len(batch)} days)")

    result_df = spark.sql(sql_sp_eval(batch_start, batch_end)).dropDuplicates(["cusip", "eval_date", "eval_hour"])

    if result_df.isEmpty():
        print(f"  No evals — skipping.")
        continue

    result_df.createOrReplaceTempView("__sp_eval_batch_tmp")

    if not spark.catalog.tableExists(TARGET_TABLE):
        result_df.write.format("delta").saveAsTable(TARGET_TABLE)
    else:
        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} t
            USING __sp_eval_batch_tmp s
            ON t.cusip = s.cusip
                AND t.eval_date = s.eval_date
                AND t.eval_hour = s.eval_hour
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    print(f"  Done.")

print("Backfill complete.")
