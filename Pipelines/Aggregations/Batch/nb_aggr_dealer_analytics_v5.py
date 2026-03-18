# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from functools import reduce
from datetime import date
from datetime import datetime
import pytz

# COMMAND ----------

# --- CONFIGURATION ---
#CALC_START_DATE = '2026-02-01'   # First calc date in range
#CALC_END_DATE   = '2026-02-25'   # Last calc date in range
#CALC_START_DATE = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
#CALC_END_DATE   = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

est = pytz.timezone('US/Eastern')
today_est = datetime.now(est).strftime('%Y-%m-%d')

CALC_START_DATE = today_est
CALC_END_DATE   = today_est


LOOKBACK_COUNT  = 10             # Number of trading days per window
TARGET_TABLE    = "refined_us_corporates.aggr_dealer_metrics"

# Trade size thresholds per spec (Metric 2)
IG_MIN_SIZE = 500_000
HY_MIN_SIZE = 500_000

# Metric 3 filters per spec
MIN_DEALERS_QUOTING = 3
# NOTE: The $250k size filter is handled by using the Px_Sz / Sprd_Sz TOB columns,
# which are pre-computed by the TOB pipeline for quotes with size only.

# COMMAND ----------

# --- 1. RESOLVE TRADING DATES ---
df_trading_days = spark.table("trusted_us_corporates.trading_days")

# Get all trading dates that could be a calc_date in our range
calc_dates_rows = (
    df_trading_days
    .filter(
        (F.col("tradingDate") >= F.lit(CALC_START_DATE)) &
        (F.col("tradingDate") <= F.lit(CALC_END_DATE))
    )
    .orderBy("tradingDate")
    .select("tradingDate")
    .collect()
)
calc_dates = [row['tradingDate'] for row in calc_dates_rows]

# Pre-fetch all trading dates up to CALC_END_DATE for lookback window resolution
all_dates_rows = (
    df_trading_days
    .filter(F.col("tradingDate") <= F.lit(CALC_END_DATE))
    .orderBy("tradingDate")
    .select("tradingDate")
    .collect()
)
all_trading_dates = [row['tradingDate'] for row in all_dates_rows]

# Build lookup: calc_date -> list of LOOKBACK_COUNT trading dates ending on calc_date
def get_lookback_window(calc_date, all_dates, lookback):
    """Return the last `lookback` trading dates up to and including calc_date."""
    eligible = [d for d in all_dates if d <= calc_date]
    return eligible[-lookback:] if len(eligible) >= lookback else eligible

date_windows = {}
for cd in calc_dates:
    date_windows[cd] = get_lookback_window(cd, all_trading_dates, LOOKBACK_COUNT)

# Full data range needed (earliest lookback start to latest calc_date)
earliest_data_date = min(w[0] for w in date_windows.values())
latest_data_date   = max(calc_dates)

print(f"Calc date range: {calc_dates[0]} to {calc_dates[-1]} ({len(calc_dates)} trading days)")

# COMMAND ----------

# --- 2. LOAD DATA (once, for the full range) ---
TRACE_LOOKBACK_FLOOR = '2025-06-01'  # Far enough back for illiquid bonds

df_trace = spark.table("trusted_us_corporates.vw_trace").filter(
    (F.col("trade_date") >= TRACE_LOOKBACK_FLOOR) &
    (F.col("trade_date") <= latest_data_date)
).cache()
df_tob = spark.table("hive_metastore.refined_us_corporates.tob_dynamic_2").filter(
    (F.to_date("ts_seq") >= earliest_data_date) &
    (F.to_date("ts_seq") <= latest_data_date)
).withColumn("snap_date", F.to_date("ts_seq")).cache()

df_instr = spark.table("bondstatic.vw_instrument_historic_versions")

# Convention lookup (deterministic, latest per CUSIP)
w_convention = Window.partitionBy("cusip").orderBy(F.col("historical_id").desc())
cusip_convention = (
    df_instr
    .withColumn("rn", F.row_number().over(w_convention))
    .filter(F.col("rn") == 1)
    .select("cusip", F.col("quote_convention_flag").alias("convention"))
).cache()

print("Data loaded and cached.")

# COMMAND ----------

# ==============================================================================
# METRIC FUNCTIONS
# ==============================================================================

def calc_metric1_liquidity(df_tob_window, lookback_count):
    """
    Metric 1: Average Displayed Liquidity.
    For each cusip/firm/side: max daily quantity (from non-stale TOB stack entries)
    averaged over the lookback window.
    """
    liq_daily_max = (
        df_tob_window
        .select("cusip", "snap_date", F.explode("stack").alias("q"))
        .filter(F.col("q.is_stale") == False)
        .select(
            "cusip", "snap_date",
            F.col("q.firm").alias("firm"),
            F.col("q.side").alias("side"),
            F.coalesce(F.col("q.quantity").cast("double"), F.lit(0.0)).alias("qty"),
        )
        .groupBy("cusip", "firm", "snap_date", "side")
        .agg(F.max("qty").alias("daily_max_qty"))
    )

    metric1 = (
        liq_daily_max.groupBy("cusip", "firm", "side")
        .agg((F.sum("daily_max_qty") / F.lit(lookback_count)).alias("avg_displayed_liquidity"))
    )

    # Pivot to wide format
    return (
        metric1.groupBy("cusip", "firm")
        .pivot("side", ["bid", "ask"])
        .sum("avg_displayed_liquidity")
        .withColumnRenamed("bid", "liq_bid")
        .withColumnRenamed("ask", "liq_ask")
    )


def calc_metric2_aggression(df_trace_cached, df_tob_cached, df_instr, calc_date):
    """
    Metric 2: Aggression score.
    Last 10 qualifying DBC trades per CUSIP for bid scoring.
    Last 10 qualifying DSC trades per CUSIP for ask scoring.
    Convention-aware: price uses price delta, spread uses spread delta.
    Uses the TOB snapshot immediately preceding each trade so that stale
    quotes (q.is_stale == True) are excluded.

    A dealer's quote only scores for a given trade if they have a non-null,
    non-zero value for the relevant convention field (price or spread).
    Dealers quoting in the wrong convention for a bond are silently excluded
    from that trade's scoring — they are never awarded a synthetic 0.

    Output per side (bid from DBC, ask from DSC):
      *_lookback_trades  - total trades in the window for that CUSIP (≤ TRADE_LOOKBACK)
      *_trades_qualified - trades where this dealer had a valid scoreable quote
      *_pct_qualified    - qualified / lookback * 100
      agg_*_avg          - mean score across qualified trades
      agg_*_std          - sample std dev of scores (consistency)
      agg_*_mae          - mean absolute error (avg deviation regardless of direction)
    """
    TRADE_LOOKBACK = 10  # Last N trades per CUSIP per flow type

    # 1. All qualifying trades up to calc_date
    trace_base = (
        df_trace_cached.alias("a")
        .join(df_instr.alias("b"), F.col("a.historical_id") == F.col("b.historical_id"), "inner")
        .filter(
            (F.col("a.trade_date") <= calc_date) &
            (F.col("a.trade_flow").isin("DBC", "DSC")) &
            (
                (F.col("b.quote_convention_flag") == 'price') |
                ((F.col("b.quote_convention_flag") == 'spread') & F.col("a.trade_spread").isNotNull())
            )
        )
        .filter(
            F.when(
                F.col("b.quote_convention_flag") == 'spread',
                F.col("a.trade_size") >= IG_MIN_SIZE
            ).otherwise(
                F.col("a.trade_size") >= HY_MIN_SIZE
            )
        )
        .select(
            F.col("a.cusip").alias("trd_cusip"),
            F.col("a.trade_timestamp").alias("trd_ts"),
            F.col("a.trade_price").alias("trd_price"),
            F.col("a.trade_spread").alias("trd_spread"),
            F.col("a.trade_flow").alias("trd_flow"),
            F.col("b.quote_convention_flag").alias("trd_convention")
        )
    )

    if trace_base.isEmpty():
        return spark.createDataFrame([], (
            "cusip STRING, firm STRING, "
            "bid_lookback_trades LONG, bid_trades_qualified LONG, bid_pct_qualified DOUBLE, "
            "agg_bid_avg DOUBLE, agg_bid_std DOUBLE, agg_bid_mae DOUBLE, "
            "ask_lookback_trades LONG, ask_trades_qualified LONG, ask_pct_qualified DOUBLE, "
            "agg_ask_avg DOUBLE, agg_ask_std DOUBLE, agg_ask_mae DOUBLE"
        ))

    # 2. Last N DBC trades per CUSIP (for bid scoring)
    w_top = Window.partitionBy("trd_cusip").orderBy(F.col("trd_ts").desc())

    dbc_trades = (
        trace_base.filter(F.col("trd_flow") == "DBC")
        .withColumn("rn", F.row_number().over(w_top))
        .filter(F.col("rn") <= TRADE_LOOKBACK)
        .drop("rn")
    )

    # Last N DSC trades per CUSIP (for ask scoring)
    dsc_trades = (
        trace_base.filter(F.col("trd_flow") == "DSC")
        .withColumn("rn", F.row_number().over(w_top))
        .filter(F.col("rn") <= TRADE_LOOKBACK)
        .drop("rn")
    )

    # 3. Explode TOB stack, keeping only non-stale quotes.
    #    Each row is one dealer's live quote at a given snapshot time.
    tob_exploded = (
        df_tob_cached
        .select("cusip", "ts_seq", F.explode("stack").alias("q"))
        .filter(F.col("q.is_stale") == False)
        .select(
            "cusip", "ts_seq",
            F.col("q.firm").alias("firm"),
            F.col("q.side").alias("side"),
            F.col("q.price").cast("double").alias("price"),
            F.col("q.spread").cast("double").alias("spread"),
        )
    )

    # For each (cusip, trade, firm): pick the TOB snapshot immediately before the trade.
    w_latest = Window.partitionBy("trd_cusip", "trd_ts", "firm").orderBy(F.col("ts_seq").desc())

    def _score_col():
        """
        Compute per-trade score. Returns null (not 0) when the dealer's quote
        field is missing or zero for the bond's convention — those rows are
        excluded from all aggregations and qualifying counts.
        """
        return (
            F.when(
                (F.col("trd_convention") == 'price') &
                F.col("price").isNotNull() & (F.col("price") != 0),
                F.col("price") - F.col("trd_price")
            ).when(
                (F.col("trd_convention") == 'spread') &
                F.col("spread").isNotNull() & (F.col("spread") != 0),
                F.col("spread") - F.col("trd_spread")
            )
            # otherwise → null: dealer not quoting in the required convention
        )

    # --- BID SIDE: bid TOB quotes vs DBC trades ---
    bid_tob = tob_exploded.filter(F.col("side") == "bid")

    bid_latest = (
        dbc_trades.join(
            bid_tob,
            (dbc_trades.trd_cusip == bid_tob.cusip) &
            (bid_tob.ts_seq <= dbc_trades.trd_ts),
            "inner"
        )
        .withColumn("rank", F.row_number().over(w_latest))
        .filter(F.col("rank") == 1).drop("rank")
        .withColumn("score", _score_col())
        .filter(F.col("score").isNotNull())  # exclude unscoreable quotes; never award a synthetic 0
    )

    dbc_total = dbc_trades.groupBy("trd_cusip").agg(
        F.count("*").alias("bid_lookback_trades")
    )

    scored_bid = (
        bid_latest
        .groupBy("trd_cusip", "firm")
        .agg(
            F.count("*").alias("bid_trades_qualified"),
            F.avg("score").alias("agg_bid_avg"),
            F.stddev("score").alias("agg_bid_std"),
            F.avg(F.abs(F.col("score"))).alias("agg_bid_mae"),
        )
        .join(dbc_total, "trd_cusip")
        .withColumn("bid_pct_qualified",
            F.col("bid_trades_qualified") / F.col("bid_lookback_trades") * 100)
        .withColumnRenamed("trd_cusip", "cusip")
    )

    # --- ASK SIDE: ask TOB quotes vs DSC trades ---
    ask_tob = tob_exploded.filter(F.col("side") == "ask")

    ask_latest = (
        dsc_trades.join(
            ask_tob,
            (dsc_trades.trd_cusip == ask_tob.cusip) &
            (ask_tob.ts_seq <= dsc_trades.trd_ts),
            "inner"
        )
        .withColumn("rank", F.row_number().over(w_latest))
        .filter(F.col("rank") == 1).drop("rank")
        .withColumn("score", _score_col())
        .filter(F.col("score").isNotNull())
    )

    dsc_total = dsc_trades.groupBy("trd_cusip").agg(
        F.count("*").alias("ask_lookback_trades")
    )

    scored_ask = (
        ask_latest
        .groupBy("trd_cusip", "firm")
        .agg(
            F.count("*").alias("ask_trades_qualified"),
            F.avg("score").alias("agg_ask_avg"),
            F.stddev("score").alias("agg_ask_std"),
            F.avg(F.abs(F.col("score"))).alias("agg_ask_mae"),
        )
        .join(dsc_total, "trd_cusip")
        .withColumn("ask_pct_qualified",
            F.col("ask_trades_qualified") / F.col("ask_lookback_trades") * 100)
        .withColumnRenamed("trd_cusip", "cusip")
    )

    # --- Combine bid + ask ---
    return scored_bid.join(scored_ask, ["cusip", "firm"], "outer")


def calc_metric3_specialist(df_tob_window, cusip_convention):
    """
    Metric 3: Specialist Rank.
    Explode the stack to find ALL non-stale dealers tied at the best bid/offer level.
    Convention-aware: price convention ranks by price, spread convention ranks by spread.
    All dealers tied at the best level get credit (not just the tiebreak winner).
    """
    # 1. Explode stack, keep only non-stale quotes
    tob_exploded = (
        df_tob_window
        .select("cusip", "ts_seq", "snap_date", "price_type", F.explode("stack").alias("q"))
        .filter(F.col("q.is_stale") == False)
        .select(
            "cusip", "ts_seq", "snap_date", "price_type",
            F.col("q.firm").alias("firm"),
            F.col("q.side").alias("side"),
            F.col("q.price").cast("double").alias("price"),
            F.col("q.spread").cast("double").alias("spread"),
        )
    )

    # 2. Join with convention to determine ranking method
    tob_with_conv = tob_exploded.join(cusip_convention, "cusip", "left")
    tob_with_conv = tob_with_conv.withColumn(
        "eff_convention", F.coalesce(F.col("convention"), F.col("price_type"))
    )

    # 3. Compute the "competitiveness" value per quote
    #    For bids:  price convention -> highest price is best; spread convention -> lowest spread is best
    #    For asks:  price convention -> lowest price is best;  spread convention -> lowest spread is best
    #
    #    We compute the best value per snapshot/side, then join back to find all dealers matching it.
    tob_with_val = tob_with_conv.withColumn("rank_val",
        F.when(F.col("side") == "bid",
            F.when(F.col("eff_convention") == "price", F.col("price"))
             .otherwise(-F.col("spread"))  # negate: higher = tighter = better
        ).when(F.col("side") == "ask",
            F.when(F.col("eff_convention") == "price", -F.col("price"))  # negate: higher = cheaper = better
             .otherwise(-F.col("spread"))  # lower spread = better
        )
    ).filter(F.col("rank_val").isNotNull())  # drop quotes missing the relevant field

    # 4. Find the best rank_val per snapshot + side
    best_per_snap = (
        tob_with_val
        .groupBy("cusip", "ts_seq", "snap_date", "side")
        .agg(F.max("rank_val").alias("best_val"))
    )

    # 5. Join back to get ALL dealers tied at the best level
    winners = (
        tob_with_val
        .join(best_per_snap, ["cusip", "ts_seq", "snap_date", "side"], "inner")
        .filter(F.col("rank_val") == F.col("best_val"))
        .select("cusip", "ts_seq", "snap_date", "side", "firm")
    )

    # 6. BID side: daily %, then average across days
    bid_winners = winners.filter(F.col("side") == "bid")

    bid_daily_total = bid_winners.groupBy("cusip", "snap_date").agg(
        F.countDistinct("ts_seq").alias("daily_total")
    )
    bid_daily_wins = bid_winners.groupBy("cusip", "snap_date", "firm").agg(
        F.countDistinct("ts_seq").alias("daily_wins")
    )
    bid_daily_pct = (
        bid_daily_wins.join(bid_daily_total, ["cusip", "snap_date"])
        .withColumn("daily_pct", (F.col("daily_wins") / F.col("daily_total")) * 100)
    )
    metric3_bid = bid_daily_pct.groupBy("cusip", "firm") \
        .agg(F.avg("daily_pct").alias("pct_best_bid"))

    # 7. ASK side: daily %, then average across days
    ask_winners = winners.filter(F.col("side") == "ask")

    ask_daily_total = ask_winners.groupBy("cusip", "snap_date").agg(
        F.countDistinct("ts_seq").alias("daily_total")
    )
    ask_daily_wins = ask_winners.groupBy("cusip", "snap_date", "firm").agg(
        F.countDistinct("ts_seq").alias("daily_wins")
    )
    ask_daily_pct = (
        ask_daily_wins.join(ask_daily_total, ["cusip", "snap_date"])
        .withColumn("daily_pct", (F.col("daily_wins") / F.col("daily_total")) * 100)
    )
    metric3_ask = ask_daily_pct.groupBy("cusip", "firm") \
        .agg(F.avg("daily_pct").alias("pct_best_ask"))

    # 8. Combine
    return (
        metric3_bid.join(metric3_ask, ["cusip", "firm"], "outer")
        .fillna(0, subset=["pct_best_bid", "pct_best_ask"])
    )

# COMMAND ----------

# ==============================================================================
# MAIN LOOP: Process each calc_date and write immediately
# ==============================================================================

for i, calc_date in enumerate(calc_dates):
    window_dates = date_windows[calc_date]
    w_start = window_dates[0]
    w_end   = window_dates[-1]
    actual_lookback = len(window_dates)

    print(f"\n[{i+1}/{len(calc_dates)}] calc_date={calc_date}, window={w_start} to {w_end} ({actual_lookback} days)")

    # Filter TOB to this calc_date's lookback window (M1, M3)
    # M2 uses full cached df_tob + calc_date for last-10-trades logic
    tob_w = df_tob.filter(
        (F.col("snap_date") >= w_start) & (F.col("snap_date") <= w_end)
    )

    # Compute metrics
    m1 = calc_metric1_liquidity(tob_w, actual_lookback)
    m2 = calc_metric2_aggression(df_trace, df_tob, df_instr, calc_date)
    m3 = calc_metric3_specialist(tob_w, cusip_convention)

    # Merge all metrics for this calc_date
    # fillna(0) is scoped to liquidity and specialist only — aggression nulls mean
    # "not evaluated" and must not be zeroed (0 would imply a perfect score).
    daily_result = (
        m1.join(m2, ["cusip", "firm"], "outer")
          .join(m3, ["cusip", "firm"], "outer")
          .fillna(0, subset=["liq_bid", "liq_ask", "pct_best_bid", "pct_best_ask"])
          .filter(F.col("firm").isNotNull())
          .withColumn("calc_date", F.lit(calc_date).cast("date"))
          .select(
              "calc_date", "cusip", "firm",
              "liq_bid", "liq_ask",
              "bid_lookback_trades", "bid_trades_qualified", "bid_pct_qualified",
              "agg_bid_avg", "agg_bid_std", "agg_bid_mae",
              "ask_lookback_trades", "ask_trades_qualified", "ask_pct_qualified",
              "agg_ask_avg", "agg_ask_std", "agg_ask_mae",
              "pct_best_bid", "pct_best_ask"
          )
    )

    # Write this day immediately — replaceWhere makes it idempotent on reruns
    if i == 0 and not spark.catalog.tableExists(TARGET_TABLE):
        daily_result.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(TARGET_TABLE)
    else:
        daily_result.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"calc_date = '{calc_date}'") \
            .option("mergeSchema", "true") \
            .saveAsTable(TARGET_TABLE)

    row_count = spark.table(TARGET_TABLE).filter(f"calc_date = '{calc_date}'").count()
    print(f"  -> wrote {row_count} rows for {calc_date}")
