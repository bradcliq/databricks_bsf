-- Databricks notebook source
CREATE MATERIALIZED VIEW aggr_quotes_firm_current_day
TBLPROPERTIES("delta.autoOptimize.optimizeWrite"="true",
    "delta.autoOptimize.autoCompact"="true",
    "pipelines.trigger.interval" = "1 minute") -- RECOMMENDATION: Increase this interval (e.g., "15 minutes")
AS
WITH
-- CTE to define time constants ONCE, ensuring consistency
constants AS (
    SELECT
        from_utc_timestamp(current_timestamp(), 'America/New_York') AS now_EST,
        -- Calculate the start/end of the current EST day in UTC for a sargable filter
        to_utc_timestamp(date(from_utc_timestamp(current_timestamp(), 'America/New_York')), 'America/New_York') AS start_of_day_EST_in_UTC,
        to_utc_timestamp(date_add(date(from_utc_timestamp(current_timestamp(), 'America/New_York')), 1), 'America/New_York') AS start_of_next_day_EST_in_UTC
),
-- Use constants to generate the hourly intervals
today_intervals AS (
    SELECT
        date(c.now_EST) AS quote_date_EST,
        explode(sequence(6, LEAST(hour(c.now_EST), 18))) AS hour_EST
    FROM constants c
),
-- Get all of today's quotes, joining with bond static data
-- This CTE is much more efficient due to the sargable WHERE clause
today_quotes_with_bondstatic AS (
    SELECT
        q1.instrument_historical_identity,
        q1.cusip,
        q1.firm,
        q1.side,
        q1.price,
        q1.spread,
        q1.quantity,
        date(from_utc_timestamp(q1.quote_timestamp_UTC, 'America/New_York')) AS quote_date_EST,
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
        cast((date(from_utc_timestamp(q1.quote_timestamp_UTC, 'America/New_York')) - b1.when_issued) AS int) AS seasoning_days
    FROM trusted_us_corporates.vw_quotes q1
    JOIN bondstatic.vw_instrument_historic_versions b1
        ON q1.cusip = b1.cusip AND q1.instrument_historical_identity = b1.historical_id
    CROSS JOIN constants c -- Get the pre-calculated time bounds
    -- This filter is sargable and much more efficient
    WHERE q1.quote_timestamp_UTC >= c.start_of_day_EST_in_UTC
      AND q1.quote_timestamp_UTC < c.start_of_next_day_EST_in_UTC
),
-- *** THE CORE OPTIMIZATION ***
-- Perform the expensive "exploding join" only ONCE.
all_hourly_cumulative_quotes AS (
    SELECT
        hi.hour_EST AS target_hour_EST, -- This is the 'as-of' hour
        qd.*
    FROM today_intervals hi
    JOIN today_quotes_with_bondstatic qd
        ON qd.quote_date_EST = hi.quote_date_EST
        AND qd.quote_hour_EST <= hi.hour_EST
),
-- *** MODIFIED CTE ***
-- Pre-rank quotes to find the latest for each firm/side/hour AND overall
cte_ranked_hourly_quotes AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY cusip, firm, side, target_hour_EST
            ORDER BY quote_timestamp_UTC DESC
        ) AS rnk_side, -- Renamed from 'rnk'
        row_number() OVER (
            PARTITION BY cusip, firm, target_hour_EST -- No 'side'
            ORDER BY quote_timestamp_UTC DESC
        ) AS rnk_overall -- New rank for non-side-specific attributes
    FROM all_hourly_cumulative_quotes
),
-- *** MODIFIED CTE ***
-- Calculate aggregates, now reading from the optimized CTE
cte_groupbycusipfirm AS (
    SELECT
        quote_date_EST,
        target_hour_EST AS quote_hour_EST,
        cusip,
        firm,
        -- Use max(CASE WHEN rnk_overall = 1 ...) as a replacement for arg_max
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

        -- Standard counts (no change)
        count(CASE WHEN side = 'ask' THEN 1 ELSE NULL END) AS num_ask,
        count(CASE WHEN side = 'bid' THEN 1 ELSE NULL END) AS num_bid,
        count(*) AS num_quotes,
        count(distinct email_id) AS num_runs,
        count(CASE WHEN side = 'ask' AND COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_ask_with_size,
        count(CASE WHEN side = 'bid' AND COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_bid_with_size,
        count(CASE WHEN COALESCE(quantity, 0) > 0 THEN 1 ELSE NULL END) AS num_quotes_with_size,
        count(distinct CASE WHEN COALESCE(quantity, 0) > 0 THEN email_id ELSE NULL END) AS num_runs_with_size,
        
        -- Logic now uses rnk_side=1
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
-- Calculate market metrics, now reading from the optimized CTE
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
        FROM all_hourly_cumulative_quotes -- *** READS FROM OPTIMIZED CTE ***
        GROUP BY quote_date_EST, target_hour_EST, cusip, firm, email_id
    ) AS aggregated
    GROUP BY quote_date_EST, quote_hour_EST, cusip, firm
),
-- Calculate 'all' metrics, now reading from the optimized CTE
cte_all AS (
    SELECT
        quote_date_EST,
        target_hour_EST AS quote_hour_EST,
        cusip,
        count(distinct firm) AS dlr_ct_tot
    FROM all_hourly_cumulative_quotes -- *** READS FROM OPTIMIZED CTE ***
    GROUP BY quote_date_EST, target_hour_EST, cusip
),
-- Yesterday's data (no change)
cte_yesterday AS (
    SELECT
        cusip AS yest_cusip,
        firm AS yest_firm,
        firm_last_ask_price AS yest_firm_last_ask_price,
        firm_last_bid_price AS yest_firm_last_bid_price,
        firm_last_ask_spread AS yest_firm_last_ask_spread,
        firm_last_bid_spread AS yest_firm_last_bid_spread
    FROM refined_us_corporates.vw_aggr_quotes_firm_historic
    WHERE quote_date_EST = default.prior_business_day_EST_offset_6hr()
    AND quote_hour_EST = 18
)
-- Final SELECT (no change, just joins the optimized CTEs)
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
    ON a.cusip = b.cusip
    AND a.firm = b.firm
    AND a.quote_date_EST = b.quote_date_EST
    AND a.quote_hour_EST = b.quote_hour_EST
JOIN cte_all c
    ON a.cusip = c.cusip
    AND a.quote_date_EST = c.quote_date_EST
    AND a.quote_hour_EST = c.quote_hour_EST
LEFT JOIN trusted_us_corporates.dealers d
    ON a.last_from_email = d.email
LEFT JOIN cte_yesterday yest
    ON a.cusip = yest.yest_cusip
    AND a.firm = yest.yest_firm;
