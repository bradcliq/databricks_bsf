# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Backfill: Quotes Aggregations (Historical)
# MAGIC One-time backfill for the 3 quote aggregation tables:
# MAGIC - `aggr_contributingfirm_stream`
# MAGIC - `aggr_quotes_stream`
# MAGIC - `aggr_quotes_firm_stream`
# MAGIC
# MAGIC Processes trading days in configurable batch sizes. Safe to re-run (uses MERGE).
# MAGIC Run on a large cluster for best performance.

# COMMAND ----------

# -- Configuration --
TARGET_TABLE_CONTRIBUTING_FIRM = "refined_us_corporates.aggr_contributingfirm_stream"
TARGET_TABLE_QUOTES = "refined_us_corporates.aggr_quotes_stream"
TARGET_TABLE_QUOTES_FIRM = "refined_us_corporates.aggr_quotes_firm_stream"

START_DATE = "2024-03-01"
END_DATE = "2026-03-12"
BATCH_SIZE_DAYS = 5  # trading days per batch — increase on bigger clusters

# COMMAND ----------

# ============================================================================
# Get list of trading days to process
# ============================================================================

trading_days = [
    row["tradingDate"]
    for row in spark.sql(f"""
        SELECT tradingDate
        FROM raw_data.trading_days
        WHERE tradingDate BETWEEN DATE '{START_DATE}' AND DATE '{END_DATE}'
        ORDER BY tradingDate
    """).collect()
]

print(f"Processing {len(trading_days)} trading days from {trading_days[0]} to {trading_days[-1]}")

# COMMAND ----------

# ============================================================================
# SQL: Contributing Firm (batch version — all dates in batch at once)
# ============================================================================

def sql_contributing_firm(batch_start, batch_end):
    return f"""
    WITH cte_3stack AS (
        SELECT quote_date_est, cusip
        FROM trusted_us_corporates.vw_quotes
        WHERE quote_date_est BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
        GROUP BY quote_date_est, cusip
        HAVING COUNT(DISTINCT firm) >= 3
    )
    SELECT quote_date_est AS quote_date_EST, cusip, 'all' AS firm
    FROM cte_3stack

    UNION ALL

    SELECT q.quote_date_est AS quote_date_EST, q.cusip, q.firm
    FROM (
        SELECT DISTINCT quote_date_est, cusip, firm
        FROM trusted_us_corporates.vw_quotes
        WHERE quote_date_est BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ) q
    LEFT JOIN cte_3stack s
        ON q.quote_date_est = s.quote_date_est AND q.cusip = s.cusip
    WHERE s.cusip IS NULL
    """

# COMMAND ----------

# ============================================================================
# SQL: Quotes (batch version — all hours 6-18 for each date in batch)
# ============================================================================

def sql_quotes(batch_start, batch_end):
    return f"""
    WITH hourly_intervals AS (
        SELECT tradingDate AS quote_date_EST, h AS hour_EST
        FROM raw_data.trading_days
        CROSS JOIN (SELECT explode(sequence(6, 18)) AS h)
        WHERE tradingDate BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ),
    cte_prior_trading_days AS (
        SELECT tradingDate,
               LAG(tradingDate) OVER (ORDER BY tradingDate) AS prior_trading_date
        FROM raw_data.trading_days
        WHERE tradingDate BETWEEN DATE '{batch_start}' - INTERVAL 10 DAYS AND DATE '{batch_end}'
    ),
    cte_quotes_filtered AS (
        SELECT q.*
        FROM trusted_us_corporates.vw_quotes q
        WHERE q.quote_date_EST BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ),
    cte_historic_quotes AS (
        SELECT
            q.quote_identity,
            q.instrument_historical_identity,
            q.quote_timestamp_UTC,
            q.quote_date_EST,
            q.email_id,
            q.side,
            q.firm,
            q.quantity,
            hour(from_utc_timestamp(q.quote_timestamp_UTC, 'America/New_York')) AS quote_hour_EST,
            hi.hour_EST,
            b.cusip,
            b.figi,
            b.isin,
            b.maturity_date,
            b.issue_date,
            b.bloomberg_ticker,
            b.composite_symbol,
            b.composite_score,
            b.bcq_sector,
            b.quote_convention_flag,
            b.issue_type,
            b.issue_name,
            b.current_coupon,
            b.payment_category,
            b.is_144a,
            b.is_regs,
            b.is_registered
        FROM cte_quotes_filtered q
        JOIN bondstatic.vw_instrument_historic_versions b
            ON q.instrument_historical_identity = b.historical_id
        JOIN hourly_intervals hi
            ON q.quote_date_EST = hi.quote_date_EST
            AND hour(from_utc_timestamp(q.quote_timestamp_UTC, 'America/New_York')) <= hi.hour_EST
    ),
    cte_bid_ask_counts AS (
        SELECT
            cusip,
            quote_date_EST,
            hour_EST,
            COUNT(DISTINCT CASE WHEN side = 'bid' THEN firm END) AS dlr_ct_bid,
            COUNT(DISTINCT CASE WHEN side = 'ask' THEN firm END) AS dlr_ct_ask,
            COUNT(DISTINCT firm) AS dlr_ct_tot,
            COUNT(DISTINCT CASE WHEN side = 'bid' AND quantity > 0 THEN firm END) AS dlr_size_ct_bid,
            COUNT(DISTINCT CASE WHEN side = 'ask' AND quantity > 0 THEN firm END) AS dlr_size_ct_ask,
            COUNT(DISTINCT CASE WHEN quantity > 0 THEN firm END) AS dlr_size_ct_tot,
            COUNT(CASE WHEN side = 'bid' THEN 1 END) AS num_bids,
            COUNT(CASE WHEN side = 'ask' THEN 1 END) AS num_ask,
            COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 5 THEN firm END) AS dlr_ct_tot_6hr,
            COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 2 THEN firm END) AS dlr_ct_tot_3hr,
            COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 1 THEN firm END) AS dlr_ct_tot_2hr,
            COUNT(DISTINCT CASE WHEN quote_hour_EST = hour_EST THEN firm END) AS dlr_ct_tot_1hr
        FROM cte_historic_quotes
        GROUP BY cusip, quote_date_EST, hour_EST
    ),
    cte_bid_ask_firms AS (
        SELECT
            cusip,
            quote_date_EST,
            hour_EST,
            COUNT(firm) AS dlr_ct_bid_ask
        FROM (
            SELECT cusip, firm, quote_date_EST, hour_EST, COUNT(DISTINCT side) AS side_count
            FROM cte_historic_quotes
            GROUP BY cusip, firm, quote_date_EST, hour_EST
        ) sub
        WHERE side_count = 2
        GROUP BY cusip, quote_date_EST, hour_EST
    ),
    cte_markets AS (
        SELECT cusip, quote_date_EST, hour_EST, SUM(ctmarket) AS markets
        FROM (
            SELECT cusip, email_id, quote_date_EST, hour_EST,
                CASE WHEN SUM(CASE WHEN side = 'ask' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END +
                CASE WHEN SUM(CASE WHEN side = 'bid' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS ctmarket
            FROM cte_historic_quotes
            GROUP BY cusip, email_id, quote_date_EST, hour_EST
        ) sub
        GROUP BY cusip, quote_date_EST, hour_EST
    ),
    cte_dealeragg AS (
        SELECT
            cusip,
            AVG(firmquotecount) AS avg_firmquotecount,
            quote_date_EST,
            hour_EST
        FROM (
            SELECT cusip, firm, COUNT(*) AS firmquotecount, quote_date_EST, hour_EST
            FROM cte_historic_quotes
            GROUP BY cusip, firm, quote_date_EST, hour_EST
        ) sub
        GROUP BY cusip, quote_date_EST, hour_EST
    ),
    cte_firstbidask AS (
        SELECT
            a.cusip, a.first_bid_firm, a.first_bid_time,
            b.first_ask_firm, b.first_ask_time,
            a.quote_date_EST, a.hour_EST
        FROM (
            SELECT cusip, quote_date_EST, hour_EST,
                   MIN(quote_timestamp_UTC) AS first_bid_time,
                   MIN_BY(firm, quote_timestamp_UTC) AS first_bid_firm
            FROM cte_historic_quotes
            WHERE side = 'bid'
            GROUP BY cusip, quote_date_EST, hour_EST
        ) a
        JOIN (
            SELECT cusip, quote_date_EST, hour_EST,
                   MIN(quote_timestamp_UTC) AS first_ask_time,
                   MIN_BY(firm, quote_timestamp_UTC) AS first_ask_firm
            FROM cte_historic_quotes
            WHERE side = 'ask'
            GROUP BY cusip, quote_date_EST, hour_EST
        ) b
        ON a.cusip = b.cusip AND a.quote_date_EST = b.quote_date_EST AND a.hour_EST = b.hour_EST
    ),
    cte_yesterday AS (
        SELECT eod.cusip, eod.stack_size AS yest_dlr_ct, ptd.tradingDate AS for_date
        FROM cte_prior_trading_days ptd
        JOIN refined_us_corporates.vw_aggr_quotes_historic_EOD eod
            ON eod.quote_date_EST = ptd.prior_trading_date
        WHERE ptd.tradingDate BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    )
    SELECT
        max(quote_identity) AS max_quote_identity,
        LAST(figi) AS figi,
        LAST(main.isin) AS isin,
        LAST(instrument_historical_identity) AS historical_id,
        main.cusip,
        main.quote_date_EST,
        main.hour_EST,
        LAST(maturity_date) AS maturity_date,
        LAST(issue_date) AS issue_date,
        LAST(bloomberg_ticker) AS bloomberg_ticker,
        LAST(composite_symbol) AS credit_rating_composite,
        LAST(composite_score) AS credit_score_composite,
        LAST(bcq_sector) AS sector,
        LAST(CASE WHEN quote_convention_flag = 'spread' THEN 'IG' ELSE 'HY' END) AS quality_flag,
        LAST(issue_type) AS issue_type,
        LAST(quote_convention_flag) AS quote_price_convention,
        bac.num_bids,
        bac.num_ask,
        bac.dlr_ct_bid,
        bac.dlr_ct_ask,
        bac.dlr_ct_tot,
        bac.dlr_ct_tot_6hr,
        bac.dlr_ct_tot_3hr,
        bac.dlr_ct_tot_2hr,
        bac.dlr_ct_tot_1hr,
        CASE WHEN bac.dlr_ct_tot >= 3 THEN 3 ELSE bac.dlr_ct_tot END AS stack_size,
        COALESCE(ba.dlr_ct_bid_ask, 0) AS dlr_ct_bid_ask,
        bac.dlr_size_ct_bid,
        bac.dlr_size_ct_ask,
        bac.dlr_size_ct_tot,
        COUNT(main.cusip) AS num_quotes,
        LAST(mkt.markets) AS markets,
        LAST(da.avg_firmquotecount) AS avg_firmquotecount,
        FIRST(fba.first_ask_firm) AS first_ask_firm,
        FIRST(fba.first_bid_firm) AS first_bid_firm,
        CASE
            WHEN COALESCE(FIRST(fba.first_ask_time), '2049-01-01') >= COALESCE(FIRST(fba.first_bid_time), '2049-01-01')
            THEN FIRST(fba.first_bid_firm) ELSE FIRST(fba.first_ask_firm)
        END AS first_quote_firm,
        LAST(hive_metastore.default.get_issue_desc(main.cusip)) AS issue_desc,
        LAST(issue_name) AS issue_name,
        LAST(current_coupon) AS current_coupon,
        LAST(payment_category) AS payment_category,
        LAST(default.get_bond_type(is_144a, is_regs, is_registered)) AS bond_type,
        date_diff(main.quote_date_EST, LAST(issue_date)) AS pricing_period,
        LAST(yest.yest_dlr_ct) AS eod_dlr_ct
    FROM cte_historic_quotes main
    JOIN cte_bid_ask_counts bac
        ON main.cusip = bac.cusip AND main.quote_date_EST = bac.quote_date_EST AND main.hour_EST = bac.hour_EST
    LEFT JOIN cte_bid_ask_firms ba
        ON main.cusip = ba.cusip AND main.quote_date_EST = ba.quote_date_EST AND main.hour_EST = ba.hour_EST
    LEFT JOIN cte_markets mkt
        ON main.cusip = mkt.cusip AND main.quote_date_EST = mkt.quote_date_EST AND main.hour_EST = mkt.hour_EST
    LEFT JOIN cte_dealeragg da
        ON main.cusip = da.cusip AND main.quote_date_EST = da.quote_date_EST AND main.hour_EST = da.hour_EST
    LEFT JOIN cte_firstbidask fba
        ON main.cusip = fba.cusip AND main.quote_date_EST = fba.quote_date_EST AND main.hour_EST = fba.hour_EST
    LEFT JOIN cte_yesterday yest
        ON main.cusip = yest.cusip AND main.quote_date_EST = yest.for_date
    GROUP BY main.cusip, main.quote_date_EST, main.hour_EST,
        bac.num_bids, bac.num_ask, bac.dlr_ct_bid, bac.dlr_ct_ask, bac.dlr_ct_tot,
        bac.dlr_ct_tot_6hr, bac.dlr_ct_tot_3hr, bac.dlr_ct_tot_2hr, bac.dlr_ct_tot_1hr,
        ba.dlr_ct_bid_ask, bac.dlr_size_ct_bid, bac.dlr_size_ct_ask, bac.dlr_size_ct_tot
    """

# COMMAND ----------

# ============================================================================
# SQL: Quotes by Firm (batch version — all hours 6-18 for each date in batch)
# ============================================================================

def sql_quotes_firm(batch_start, batch_end):
    return f"""
    WITH hourly_intervals AS (
        SELECT tradingDate AS quote_date_EST, h AS hour_EST
        FROM raw_data.trading_days
        CROSS JOIN (SELECT explode(sequence(6, 18)) AS h)
        WHERE tradingDate BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ),
    cte_prior_trading_days AS (
        SELECT tradingDate,
               LAG(tradingDate) OVER (ORDER BY tradingDate) AS prior_trading_date
        FROM raw_data.trading_days
        WHERE tradingDate BETWEEN DATE '{batch_start}' - INTERVAL 10 DAYS AND DATE '{batch_end}'
    ),
    today_quotes_with_bondstatic AS (
        SELECT
            q1.instrument_historical_identity,
            q1.cusip,
            q1.firm,
            q1.side,
            q1.price,
            q1.spread,
            q1.quantity,
            q1.quote_date_EST,
            hour(from_utc_timestamp(q1.quote_timestamp_UTC, 'America/New_York')) AS quote_hour_EST,
            q1.quote_timestamp_UTC,
            q1.from_email,
            q1.email_id,
            b1.quote_convention_flag,
            q1.ISIN,
            b1.bloomberg_ticker,
            q1.Figi,
            b1.current_coupon,
            b1.maturity_date,
            b1.issue_name,
            b1.issue_subtype,
            b1.bcq_sector,
            b1.equity_ticker,
            b1.composite_score,
            default.get_sector_abbr2(b1.bcq_sector) AS sector,
            default.get_rating_bucket2(b1.composite_score) AS rtg_bucket,
            default.get_issue_desc2(b1.bloomberg_ticker, b1.current_coupon, b1.maturity_date) AS issue_desc_2,
            b1.when_issued,
            b1.payment_category,
            b1.payment_category_subtype,
            b1.classification,
            b1.tier,
            b1.current_amount_outstanding,
            default.get_mat_bucket2(b1.maturity_date) AS maturity_bucket,
            cast((q1.quote_date_EST - b1.when_issued) AS int) AS seasoning_days,
            hi.hour_EST AS target_hour_EST
        FROM trusted_us_corporates.vw_quotes q1
        JOIN bondstatic.vw_instrument_historic_versions b1
            ON q1.cusip = b1.cusip AND q1.instrument_historical_identity = b1.historical_id
        JOIN hourly_intervals hi
            ON q1.quote_date_EST = hi.quote_date_EST
            AND hour(from_utc_timestamp(q1.quote_timestamp_UTC, 'America/New_York')) <= hi.hour_EST
        WHERE q1.quote_date_EST BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    ),
    cte_ranked_hourly_quotes AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY cusip, firm, side, quote_date_EST, target_hour_EST
                ORDER BY quote_timestamp_UTC DESC
            ) AS rnk_side,
            row_number() OVER (
                PARTITION BY cusip, firm, quote_date_EST, target_hour_EST
                ORDER BY quote_timestamp_UTC DESC
            ) AS rnk_overall
        FROM today_quotes_with_bondstatic
    ),
    cte_groupbycusipfirm AS (
        SELECT
            quote_date_EST,
            target_hour_EST AS quote_hour_EST,
            cusip,
            firm,
            max(CASE WHEN rnk_overall = 1 THEN instrument_historical_identity ELSE NULL END) AS historical_id,
            max(CASE WHEN rnk_overall = 1 THEN quote_convention_flag ELSE NULL END) AS quote_convention_flag,
            max(CASE WHEN rnk_overall = 1 THEN ISIN ELSE NULL END) AS ISIN,
            max(CASE WHEN rnk_overall = 1 THEN bloomberg_ticker ELSE NULL END) AS bloomberg_ticker,
            max(CASE WHEN rnk_overall = 1 THEN Figi ELSE NULL END) AS Figi,
            max(CASE WHEN rnk_overall = 1 THEN current_coupon ELSE NULL END) AS current_coupon,
            max(CASE WHEN rnk_overall = 1 THEN maturity_date ELSE NULL END) AS maturity_date,
            max(CASE WHEN rnk_overall = 1 THEN issue_name ELSE NULL END) AS issue_name,
            max(CASE WHEN rnk_overall = 1 THEN issue_subtype ELSE NULL END) AS issue_subtype,
            max(CASE WHEN rnk_overall = 1 THEN bcq_sector ELSE NULL END) AS bcq_sector,
            max(CASE WHEN rnk_overall = 1 THEN equity_ticker ELSE NULL END) AS equity_ticker,
            max(CASE WHEN rnk_overall = 1 THEN composite_score ELSE NULL END) AS composite_score,
            max(CASE WHEN rnk_overall = 1 THEN sector ELSE NULL END) AS sector,
            max(CASE WHEN rnk_overall = 1 THEN rtg_bucket ELSE NULL END) AS rtg_bucket,
            max(CASE WHEN rnk_overall = 1 THEN issue_desc_2 ELSE NULL END) AS issue_desc_2,
            max(CASE WHEN rnk_overall = 1 THEN when_issued ELSE NULL END) AS when_issued,
            max(CASE WHEN rnk_overall = 1 THEN payment_category ELSE NULL END) AS payment_category,
            max(CASE WHEN rnk_overall = 1 THEN payment_category_subtype ELSE NULL END) AS payment_category_subtype,
            max(CASE WHEN rnk_overall = 1 THEN classification ELSE NULL END) AS classification,
            max(CASE WHEN rnk_overall = 1 THEN tier ELSE NULL END) AS tier,
            max(CASE WHEN rnk_overall = 1 THEN current_amount_outstanding ELSE NULL END) AS current_amount_outstanding,
            max(CASE WHEN rnk_overall = 1 THEN maturity_bucket ELSE NULL END) AS maturity_bucket,
            max(CASE WHEN rnk_overall = 1 THEN seasoning_days ELSE NULL END) AS seasoning_days,
            max(CASE WHEN rnk_overall = 1 THEN from_email ELSE NULL END) AS last_from_email,
            count(CASE WHEN side = 'ask' THEN 1 ELSE NULL END) AS num_ask,
            count(CASE WHEN side = 'bid' THEN 1 ELSE NULL END) AS num_bid,
            count(*) AS num_quotes,
            count(distinct email_id) AS num_runs,
            count(CASE WHEN side = 'ask' AND COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_ask_with_size,
            count(CASE WHEN side = 'bid' AND COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_bid_with_size,
            count(CASE WHEN COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_quotes_with_size,
            count(distinct CASE WHEN COALESCE(quantity, 0) > 0 THEN email_id ELSE NULL END) AS num_runs_with_size,
            max(CASE WHEN side = 'ask' AND rnk_side = 1 THEN price ELSE NULL END) AS firm_last_ask_price,
            max(CASE WHEN side = 'bid' AND rnk_side = 1 THEN price ELSE NULL END) AS firm_last_bid_price,
            max(CASE WHEN side = 'ask' AND rnk_side = 1 THEN spread ELSE NULL END) AS firm_last_ask_spread,
            max(CASE WHEN side = 'bid' AND rnk_side = 1 THEN spread ELSE NULL END) AS firm_last_bid_spread,
            max(CASE WHEN side = 'ask' AND rnk_side = 1 THEN quantity ELSE NULL END) AS firm_last_ask_quantity,
            max(CASE WHEN side = 'bid' AND rnk_side = 1 THEN quantity ELSE NULL END) AS firm_last_bid_quantity,
            max(CASE WHEN side = 'ask' AND rnk_side = 1 THEN quote_timestamp_UTC ELSE NULL END) AS firm_last_ask_timestamp_UTC,
            max(CASE WHEN side = 'bid' AND rnk_side = 1 THEN quote_timestamp_UTC ELSE NULL END) AS firm_last_bid_timestamp_UTC,
            max(CASE WHEN side = 'ask' AND rnk_side = 1 AND quote_convention_flag = 'spread' THEN spread ELSE NULL END) AS firm_last_ask_price_spread,
            max(CASE WHEN side = 'bid' AND rnk_side = 1 AND quote_convention_flag = 'spread' THEN spread ELSE NULL END) AS firm_last_bid_price_spread
        FROM cte_ranked_hourly_quotes
        GROUP BY quote_date_EST, target_hour_EST, cusip, firm
    ),
    cte_markets AS (
        SELECT
            quote_date_EST,
            quote_hour_EST,
            cusip,
            firm,
            count(CASE WHEN ask_run_count > 0 AND bid_run_count > 0 THEN 1 ELSE NULL END) AS markets,
            count(CASE WHEN ask_run_with_size_count > 0 AND bid_run_with_size_count > 0 THEN 1 ELSE NULL END) AS markets_with_size,
            avg(CASE WHEN ask_run_count > 0 AND bid_run_count > 0 THEN bid_ask_price ELSE NULL END) AS avg_bid_ask_price,
            avg(CASE WHEN ask_run_count > 0 AND bid_run_count > 0 THEN bid_ask_spread ELSE NULL END) AS avg_bid_ask_spread,
            avg(CASE WHEN ask_run_with_size_count > 0 AND bid_run_with_size_count > 0 THEN bid_ask_price ELSE NULL END) AS avg_bid_ask_price_wsize,
            avg(CASE WHEN ask_run_with_size_count > 0 AND bid_run_with_size_count > 0 THEN bid_ask_spread ELSE NULL END) AS avg_bid_ask_spread_wsize,
            count(distinct CASE WHEN ask_run_with_size_count > 0 AND bid_run_with_size_count > 0 THEN firm ELSE NULL END) AS mkt_width_dlr_ct,
            count(distinct CASE WHEN ask_run_with_size_count > 0 AND bid_run_with_size_count > 0 THEN firm ELSE NULL END) AS mkt_width_dlr_ct_wsize
        FROM (
            SELECT
                count(CASE WHEN side = 'ask' THEN 1 ELSE NULL END) AS ask_run_count,
                count(CASE WHEN side = 'bid' THEN 1 ELSE NULL END) AS bid_run_count,
                count(CASE WHEN side = 'ask' AND quantity > 0 THEN 1 ELSE NULL END) AS ask_run_with_size_count,
                count(CASE WHEN side = 'bid' AND quantity > 0 THEN 1 ELSE NULL END) AS bid_run_with_size_count,
                max(CASE WHEN side = 'ask' THEN price ELSE NULL END) - max(CASE WHEN side = 'bid' THEN price ELSE NULL END) AS bid_ask_price,
                max(CASE WHEN side = 'bid' THEN spread ELSE NULL END) - max(CASE WHEN side = 'ask' THEN spread ELSE NULL END) AS bid_ask_spread,
                quote_date_EST,
                target_hour_EST AS quote_hour_EST,
                cusip,
                firm,
                email_id
            FROM today_quotes_with_bondstatic
            GROUP BY quote_date_EST, target_hour_EST, cusip, firm, email_id
        ) aggregated
        GROUP BY quote_date_EST, quote_hour_EST, cusip, firm
    ),
    cte_all AS (
        SELECT
            quote_date_EST,
            target_hour_EST AS quote_hour_EST,
            cusip,
            count(distinct firm) AS dlr_ct_tot
        FROM today_quotes_with_bondstatic
        GROUP BY quote_date_EST, target_hour_EST, cusip
    ),
    cte_yesterday AS (
        SELECT eod.cusip AS yest_cusip, eod.firm AS yest_firm,
               eod.firm_last_ask_price AS yest_firm_last_ask_price,
               eod.firm_last_bid_price AS yest_firm_last_bid_price,
               eod.firm_last_ask_spread AS yest_firm_last_ask_spread,
               eod.firm_last_bid_spread AS yest_firm_last_bid_spread,
               ptd.tradingDate AS for_date
        FROM cte_prior_trading_days ptd
        JOIN refined_us_corporates.vw_aggr_quotes_firm_historic eod
            ON eod.quote_date_EST = ptd.prior_trading_date AND eod.quote_hour_EST = 18
        WHERE ptd.tradingDate BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
    )
    SELECT
        a.quote_date_EST,
        a.quote_hour_EST,
        a.cusip,
        a.firm,
        a.historical_id,
        a.quote_convention_flag,
        a.num_ask,
        a.num_bid,
        a.num_quotes,
        a.num_runs,
        a.num_ask_with_size,
        a.num_bid_with_size,
        a.num_quotes_with_size,
        a.num_runs_with_size,
        a.last_from_email,
        d.user,
        b.markets,
        b.avg_bid_ask_price,
        b.avg_bid_ask_spread,
        b.markets_with_size,
        b.avg_bid_ask_price_wsize,
        b.avg_bid_ask_spread_wsize,
        b.mkt_width_dlr_ct,
        b.mkt_width_dlr_ct_wsize,
        c.dlr_ct_tot,
        a.ISIN,
        a.bloomberg_ticker,
        a.Figi,
        a.current_coupon,
        a.maturity_date,
        a.issue_name,
        a.issue_subtype,
        a.bcq_sector,
        a.equity_ticker,
        a.composite_score,
        a.sector,
        a.rtg_bucket,
        a.issue_desc_2,
        a.when_issued,
        a.payment_category,
        a.payment_category_subtype,
        a.classification,
        a.tier,
        a.current_amount_outstanding,
        a.maturity_bucket,
        a.seasoning_days,
        a.firm_last_ask_price,
        a.firm_last_bid_price,
        a.firm_last_ask_spread,
        a.firm_last_bid_spread,
        a.firm_last_ask_quantity,
        a.firm_last_bid_quantity,
        a.firm_last_ask_timestamp_UTC,
        a.firm_last_bid_timestamp_UTC,
        a.firm_last_ask_price_spread,
        a.firm_last_bid_price_spread,
        yest.yest_firm_last_ask_price,
        yest.yest_firm_last_bid_price,
        yest.yest_firm_last_ask_spread,
        yest.yest_firm_last_bid_spread
    FROM cte_groupbycusipfirm a
    JOIN cte_markets b
        ON a.cusip = b.cusip AND a.firm = b.firm
        AND a.quote_date_EST = b.quote_date_EST AND a.quote_hour_EST = b.quote_hour_EST
    JOIN cte_all c
        ON a.cusip = c.cusip
        AND a.quote_date_EST = c.quote_date_EST AND a.quote_hour_EST = c.quote_hour_EST
    LEFT JOIN (SELECT email, FIRST(user) AS user FROM trusted_us_corporates.dealers GROUP BY email) d
        ON a.last_from_email = d.email
    LEFT JOIN cte_yesterday yest
        ON a.cusip = yest.yest_cusip AND a.firm = yest.yest_firm
        AND a.quote_date_EST = yest.for_date
    """

# COMMAND ----------

# ============================================================================
# Merge helpers
# ============================================================================

def _merge_or_create(result_df, target_table, merge_sql):
    if result_df.isEmpty():
        return
    result_df.createOrReplaceTempView("__batch_tmp")
    if not spark.catalog.tableExists(target_table):
        result_df.write.format("delta").saveAsTable(target_table)
    else:
        spark.sql(merge_sql)

def _upsert_contributing_firm(result_df, batch_start, batch_end):
    if result_df.isEmpty():
        return
    result_df.createOrReplaceTempView("__cf_tmp")
    if not spark.catalog.tableExists(TARGET_TABLE_CONTRIBUTING_FIRM):
        result_df.write.format("delta").saveAsTable(TARGET_TABLE_CONTRIBUTING_FIRM)
    else:
        spark.sql(f"""
            DELETE FROM {TARGET_TABLE_CONTRIBUTING_FIRM}
            WHERE quote_date_EST BETWEEN DATE '{batch_start}' AND DATE '{batch_end}'
        """)
        spark.sql(f"""
            INSERT INTO {TARGET_TABLE_CONTRIBUTING_FIRM}
            SELECT * FROM __cf_tmp
        """)

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

    # 1. Contributing Firm
    cf_df = spark.sql(sql_contributing_firm(batch_start, batch_end))
    _upsert_contributing_firm(cf_df, batch_start, batch_end)

    # 2. Quotes (by cusip)
    quotes_df = spark.sql(sql_quotes(batch_start, batch_end)).dropDuplicates(["cusip", "quote_date_EST", "hour_EST"])
    _merge_or_create(
        quotes_df,
        TARGET_TABLE_QUOTES,
        f"""
        MERGE INTO {TARGET_TABLE_QUOTES} t
        USING __batch_tmp s
        ON t.cusip = s.cusip
            AND t.quote_date_EST = s.quote_date_EST
            AND t.hour_EST = s.hour_EST
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    # 3. Quotes by Firm (by cusip + firm)
    quotes_firm_df = spark.sql(sql_quotes_firm(batch_start, batch_end)).dropDuplicates(["cusip", "firm", "quote_date_EST", "quote_hour_EST"])
    _merge_or_create(
        quotes_firm_df,
        TARGET_TABLE_QUOTES_FIRM,
        f"""
        MERGE INTO {TARGET_TABLE_QUOTES_FIRM} t
        USING __batch_tmp s
        ON t.cusip = s.cusip
            AND t.firm = s.firm
            AND t.quote_date_EST = s.quote_date_EST
            AND t.quote_hour_EST = s.quote_hour_EST
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    print(f"  Done.")

print("Backfill complete.")
