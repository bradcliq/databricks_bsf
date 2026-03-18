# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming: TRACE Trade Aggregation (Current Day)
# MAGIC Replaces the DLT materialized view `aggr_trace_current_day`.
# MAGIC
# MAGIC Computes hourly cumulative trade statistics per cusip. Only the current hour
# MAGIC is recomputed on each trigger; prior hours remain frozen. History builds across days.
# MAGIC
# MAGIC **Key efficiency gain:** Eliminates the `explode(sequence(6, current_hour))` cross-join.
# MAGIC Computes only the current hour, reading all trades up to that hour once.

# COMMAND ----------

# -- Configuration --
TARGET_TABLE = "refined_us_corporates.aggr_trace_streaming"
CHECKPOINT_PATH = "s3://use1-s3-bcq-prod-elt-raw/refined_us_corporates/checkpoints/trace_aggregation_cv_2"
TRIGGER_INTERVAL = "1 minute"

# COMMAND ----------

AGGREGATION_SQL = """
WITH constants AS (
    SELECT
        date(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS today_EST,
        LEAST(hour(from_utc_timestamp(current_timestamp(), 'America/New_York')), 18) AS current_hour_EST
),
-- Inline the view chain: vw_trace -> bondstatic -> trading_days
-- Filter on trade_date and hour BEFORE joining, so Spark can push predicates down
cte_trades_today AS (
    SELECT t.*
    FROM trusted_us_corporates.vw_trace t
    CROSS JOIN constants c
    WHERE t.trade_date = c.today_EST
      AND hour(from_utc_timestamp(t.trade_timestamp, 'America/New_York')) <= c.current_hour_EST
),
cte_trades_historic AS (
    SELECT
        t.transaction_id,
        t.trade_date,
        t.trade_timestamp,
        t.trade_price,
        t.trade_spread,
        t.trade_size,
        t.t_spread,
        t.g_spread,
        t.i_spread,
        t.z_spread,
        t.yield,
        t.benchmark_ric_name,
        t.ats_indicator,
        t.trade_flow,
        hour(from_utc_timestamp(t.trade_timestamp, 'America/New_York')) AS trade_data_hour,
        c.current_hour_EST AS trade_hour,
        b.cusip,
        b.isin,
        b.figi,
        b.historical_id,
        b.quality_flag,
        b.bloomberg_ticker,
        b.current_coupon,
        b.maturity_date,
        b.quote_convention_flag
    FROM cte_trades_today t
    JOIN bondstatic.vw_instrument_historic_versions b
        ON t.historical_id = b.historical_id
    JOIN trusted_us_corporates.trading_days td
        ON t.trade_date = td.tradingDate
    CROSS JOIN constants c
),
cte_trades_with_rank AS (
    SELECT
        DENSE_RANK() OVER (PARTITION BY trade_date, trade_hour, cusip ORDER BY transaction_id DESC) AS rn,
        CASE
            WHEN ((trade_size >= 500000) AND quality_flag = 'HY') OR (trade_size >= 1000000) THEN 1
            ELSE 0
        END AS if_inst_tr,
        *
    FROM cte_trades_historic
),
cte_trades_with_rank_ordered AS (
    SELECT
        trade_date,
        trade_hour,
        cusip,
        transaction_id,
        historical_id,
        isin,
        figi,
        quote_convention_flag,
        current_coupon,
        maturity_date,
        bloomberg_ticker,
        benchmark_ric_name,
        rn,
        trade_flow,
        trade_size,
        if_inst_tr,
        ats_indicator,
        trade_price AS trade_price_last,
        COALESCE(t_spread, trade_spread) AS trade_spread_last,
        trade_flow AS trade_flow_last,
        trade_size AS trade_size_last,
        t_spread,
        i_spread,
        g_spread,
        z_spread,
        yield,
        trade_timestamp AS trade_timestamp_last,
        CASE WHEN trade_size >= 250000 THEN trade_size ELSE NULL END AS vol_250k,
        CASE WHEN trade_size >= 500000 THEN trade_size ELSE NULL END AS vol_500k,
        CASE WHEN trade_size >= 1000000 THEN trade_size ELSE NULL END AS vol_1mm,
        CASE WHEN trade_size >= 5000000 THEN trade_size ELSE NULL END AS vol_5mm,
        CASE
            WHEN ((trade_size >= 500000 AND quality_flag = 'HY') OR (trade_size >= 1000000)) THEN 1
            ELSE NULL
        END AS trade_count_inst,
        CASE
            WHEN ((trade_size >= 500000 AND quality_flag = 'HY') OR (trade_size >= 1000000)) THEN trade_size
            ELSE NULL
        END AS trade_vol_inst
    FROM cte_trades_with_rank
    WHERE cusip IS NOT NULL
    ORDER BY trade_date, trade_hour, rn
),
cte_trades_agg1 AS (
    SELECT
        trade_date,
        trade_hour,
        cusip,
        LAST(historical_id) AS historical_id,
        LAST(isin) AS isin,
        LAST(figi) AS figi,
        LAST(quote_convention_flag) AS quote_convention_flag,
        LAST(current_coupon) AS current_coupon,
        LAST(maturity_date) AS maturity_date,
        LAST(bloomberg_ticker) AS bloomberg_ticker,
        LAST(benchmark_ric_name) AS benchmark_ric_name,
        FIRST(trade_price_last) FILTER (WHERE if_inst_tr = 1) AS trade_price_last,
        FIRST(trade_spread_last) FILTER (WHERE if_inst_tr = 1) AS trade_spread_last,
        FIRST(trade_flow_last) FILTER (WHERE if_inst_tr = 1) AS trade_flow_last,
        FIRST(trade_size_last) FILTER (WHERE if_inst_tr = 1) AS trade_size_last,
        FIRST(trade_timestamp_last) FILTER (WHERE if_inst_tr = 1) AS trade_timestamp_last,
        COUNT(trade_count_inst) AS trade_count_institutional,
        SUM(trade_vol_inst) AS trade_vol_inst,
        SUM(vol_250k) AS vol_tot_250k,
        SUM(vol_500k) AS vol_tot_500k,
        SUM(vol_1mm) AS vol_tot_1mm,
        SUM(vol_5mm) AS vol_tot_5mm,
        COUNT(*) AS trade_count,
        SUM(trade_size) AS total_trade_volume,
        SUM(trade_size_last * trade_price_last) / SUM(trade_size_last) AS vwap,
        SUM(t_spread * trade_size_last) / SUM(trade_size_last) AS vwat,
        SUM(g_spread * trade_size_last) / SUM(trade_size_last) AS vwag,
        SUM(i_spread * trade_size_last) / SUM(trade_size_last) AS vwai,
        SUM(z_spread * trade_size_last) / SUM(trade_size_last) AS vwaz,
        SUM(yield * trade_size_last) / SUM(trade_size_last) AS vwyield,
        CAST(COALESCE(SUM(CASE WHEN trade_flow = 'DSC' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS DSCvol,
        CAST(COALESCE(SUM(CASE WHEN trade_flow = 'D2D' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS D2Dvol,
        CAST(COALESCE(SUM(CASE WHEN trade_flow = 'DSA' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS DSAvol,
        CAST(COALESCE(SUM(CASE WHEN trade_flow = 'DBC' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS DBCvol,
        CAST(COALESCE(SUM(CASE WHEN trade_flow = 'DBA' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS DBAvol,
        CAST(COALESCE(SUM(CASE WHEN ats_indicator = 'Y' THEN trade_size ELSE NULL END), 0) AS DECIMAL) AS ATSvol
    FROM cte_trades_with_rank_ordered
    GROUP BY trade_date, trade_hour, cusip
),
cte_lastInstTS AS (
    SELECT
        trade_date,
        trade_hour,
        cusip,
        MAX(trade_timestamp) AS inst_last_trade_timestamp
    FROM cte_trades_historic
    WHERE (trade_size >= 500000 AND quality_flag = 'HY') OR (trade_size >= 1000000)
    GROUP BY trade_date, trade_hour, cusip
),
cte_yesterday AS (
    SELECT
        cusip AS yest_cusip,
        vwap AS yest_vwap,
        vwat AS yest_vwat
    FROM refined_us_corporates.aggr_trace_historic_calc
    WHERE trade_date = default.prior_business_day_EST_offset_6hr()
      AND trade_hour = 18
)
SELECT
    ag1.trade_date,
    ag1.trade_hour,
    ag1.cusip,
    LAST(historical_id) AS historical_id,
    LAST(isin) AS isin,
    LAST(figi) AS figi,
    LAST(quote_convention_flag) AS quote_convention_flag,
    LAST(current_coupon) AS current_coupon,
    LAST(maturity_date) AS maturity_date,
    LAST(bloomberg_ticker) AS bloomberg_ticker,
    LAST(benchmark_ric_name) AS benchmark,
    ROUND(FIRST(trade_price_last), 3) AS trade_price_last,
    ROUND(FIRST(trade_spread_last), 2) AS trade_spread_last,
    FIRST(trade_flow_last) AS trade_flow_last,
    FIRST(trade_size_last) AS trade_size_last,
    FIRST(trade_timestamp_last) AS trade_timestamp_last,
    FIRST(trade_count_institutional) AS trade_count_institutional,
    FIRST(trade_count) AS trade_count,
    FIRST(trade_vol_inst) AS trade_vol_inst,
    FIRST(total_trade_volume) AS total_trade_volume,
    FIRST(vol_tot_250k) AS vol_tot_250k,
    FIRST(vol_tot_500k) AS vol_tot_500k,
    FIRST(vol_tot_1mm) AS vol_tot_1mm,
    FIRST(vol_tot_5mm) AS vol_tot_5mm,
    FIRST(vwap) AS vwap,
    FIRST(vwat) AS vwat,
    FIRST(yest_vwap) AS yest_vwap,
    FIRST(yest_vwat) AS yest_vwat,
    FIRST(vwag) AS vwag,
    FIRST(vwai) AS vwai,
    FIRST(vwaz) AS vwaz,
    FIRST(vwyield) AS vwyield,
    FIRST(D2Dvol) AS D2Dvol,
    FIRST(DSCvol) AS DSCvol,
    FIRST(DSAvol) AS DSAvol,
    FIRST(DBCvol) AS DBCvol,
    FIRST(DBAvol) AS DBAvol,
    FIRST(ATSvol) AS ATSvol,
    (FIRST(DSCvol) + FIRST(DSAvol)) AS vol_client_buy,
    (FIRST(DBCvol) + FIRST(DBAvol)) AS vol_client_sell,
    ROUND(
        CASE WHEN (SUM(DSCvol) + SUM(DBCvol)) = 0 THEN NULL
             ELSE (SUM(DSCvol) - SUM(DBCvol)) / (SUM(DSCvol) + SUM(DBCvol)) * 100
        END,
        0
    ) AS customer_flow_pct,
    ROUND((SUM(DSCvol) - SUM(DBCvol)), 0) AS customer_flow_sum,
    FIRST(i.inst_last_trade_timestamp) AS inst_last_trade_timestamp
FROM cte_trades_agg1 ag1
LEFT JOIN cte_lastInstTS i
    ON ag1.trade_date = i.trade_date AND ag1.trade_hour = i.trade_hour AND ag1.cusip = i.cusip
LEFT JOIN cte_yesterday yest
    ON ag1.cusip = yest.yest_cusip
GROUP BY ag1.trade_date, ag1.trade_hour, ag1.cusip
"""

# COMMAND ----------

def process_batch(batch_df, batch_id):
    """Recompute trace aggregation for current hour only and merge into target."""

    current_hour = spark.sql(
        "SELECT hour(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS h"
    ).first()["h"]
    if current_hour < 6 or current_hour > 18:
        return

    result_df = spark.sql(AGGREGATION_SQL)

    if result_df.isEmpty():
        return

    result_df.createOrReplaceTempView("__trace_batch_results")

    if not spark.catalog.tableExists(TARGET_TABLE):
        result_df.write.format("delta").saveAsTable(TARGET_TABLE)
    else:
        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} t
            USING __trace_batch_results s
            ON t.cusip = s.cusip
                AND t.trade_date = s.trade_date
                AND t.trade_hour = s.trade_hour
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
    .queryName("stream_aggr_trace_current_day")
    .start()
)

stream.awaitTermination()
