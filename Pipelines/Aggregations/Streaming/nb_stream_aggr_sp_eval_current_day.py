# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming: S&P Eval Price Aggregation (Current Day)
# MAGIC Replaces the DLT materialized view `aggr_sp_eval_current_day`.
# MAGIC
# MAGIC For each cusip, keeps the latest eval price point per hour (by `asOfDateTime`).
# MAGIC Only the current hour is recomputed on each trigger; prior hours remain frozen.

# COMMAND ----------

# -- Configuration --
TARGET_TABLE = "refined_us_corporates.aggr_sp_eval_streaming"
CHECKPOINT_PATH = "s3://use1-s3-bcq-prod-elt-raw/refined_us_corporates/checkpoints/sp_eval_aggregation_cv_1"
TRIGGER_INTERVAL = "30 minute"

# COMMAND ----------

AGGREGATION_SQL = """
WITH constants AS (
    SELECT
        date(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS today_EST,
        LEAST(hour(from_utc_timestamp(current_timestamp(), 'America/New_York')), 18) AS current_hour_EST
),
cte_eval_today AS (
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
    CROSS JOIN constants c
    WHERE date(from_utc_timestamp(asOfDateTime, 'America/New_York')) = c.today_EST
      AND hour(from_utc_timestamp(asOfDateTime, 'America/New_York')) <= c.current_hour_EST
      AND cusip IS NOT NULL
      AND billingCode IN ('Corporate - HY', 'Corporate IG')
),
cte_eval_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY eval_date, cusip ORDER BY asOfDateTime DESC) AS rownum
    FROM cte_eval_today
)
SELECT
    cusip,
    asOfDateTime,
    eval_date,
    current_hour_EST AS eval_hour,
    eval_mid_spread,
    eval_mid_price,
    eval_bid_spread,
    eval_bid_price,
    eval_ask_spread,
    eval_ask_price
FROM cte_eval_ranked
CROSS JOIN (SELECT LEAST(hour(from_utc_timestamp(current_timestamp(), 'America/New_York')), 18) AS current_hour_EST)
WHERE rownum = 1
"""

# COMMAND ----------

def process_batch(batch_df, batch_id):
    """Recompute S&P eval aggregation for current hour only and merge into target."""

    current_hour = spark.sql(
        "SELECT hour(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS h"
    ).first()["h"]
    if current_hour < 6 or current_hour > 18:
        return

    result_df = spark.sql(AGGREGATION_SQL)

    if result_df.isEmpty():
        return

    result_df.createOrReplaceTempView("__sp_eval_batch_results")

    if not spark.catalog.tableExists(TARGET_TABLE):
        result_df.write.format("delta").saveAsTable(TARGET_TABLE)
    else:
        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} t
            USING __sp_eval_batch_results s
            ON t.cusip = s.cusip
                AND t.eval_date = s.eval_date
                AND t.eval_hour = s.eval_hour
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

# COMMAND ----------

# Start the stream
stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
    .writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .queryName("stream_aggr_sp_eval_current_day")
    .start()
)

stream.awaitTermination()
