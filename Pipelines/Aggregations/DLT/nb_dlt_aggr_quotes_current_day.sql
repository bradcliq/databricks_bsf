-- Databricks notebook source
CREATE MATERIALIZED VIEW aggr_quotes_current_day 
TBLPROPERTIES("delta.autoOptimize.optimizeWrite"="true",
        "delta.autoOptimize.autoCompact"="true","pipelines.trigger.interval" = "1 minute")
        AS
WITH today_intervals AS (
    SELECT
        date(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS range_date_EST,
        explode(sequence(6, LEAST(hour(from_utc_timestamp(current_timestamp(), 'America/New_York')), 18))) AS hour_EST
),
hourly_intervals AS (
    SELECT * FROM today_intervals
),
cte_historic_quotes AS (
    SELECT *,
           hour(from_utc_timestamp(quote_timestamp_UTC, 'America/New_York')) AS quote_hour_EST
    FROM trusted_us_corporates.vw_quotes_with_reference_trading_days
    WHERE date(from_utc_timestamp(quote_timestamp_UTC, 'America/New_York')) = date(from_utc_timestamp(current_timestamp(), 'America/New_York'))
),
cte_historic_quotes_cum AS (
    SELECT
        hi.hour_EST,
        hq.*
    FROM hourly_intervals hi
    LEFT JOIN cte_historic_quotes hq
        ON hq.quote_date_EST = hi.range_date_EST
        AND hq.quote_hour_EST <= hi.hour_EST
),
cte_bid_ask_counts AS (
    SELECT
        cusip,
        quote_date_EST,
        hour_EST,
        COUNT(DISTINCT CASE WHEN side = 'bid' THEN firm END) AS dlr_ct_bid,
        COUNT(DISTINCT CASE WHEN side = 'ask' THEN firm END) AS dlr_ct_ask,
        COUNT(DISTINCT firm) AS dlr_ct_tot,  -- cumulative count for entire day
        COUNT(DISTINCT CASE WHEN side = 'bid' AND quantity > 0 THEN firm END) AS dlr_size_ct_bid,
        COUNT(DISTINCT CASE WHEN side = 'ask' AND quantity > 0 THEN firm END) AS dlr_size_ct_ask,
        COUNT(DISTINCT CASE WHEN quantity > 0 THEN firm END) AS dlr_size_ct_tot,
        COUNT(CASE WHEN side = 'bid' THEN 1 END) AS num_bids,
        COUNT(CASE WHEN side = 'ask' THEN 1 END) AS num_ask,
        -- New sliding window counts:
        COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 5 THEN firm END) AS dlr_ct_tot_6hr,
        COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 2 THEN firm END) AS dlr_ct_tot_3hr,
        COUNT(DISTINCT CASE WHEN quote_hour_EST >= hour_EST - 1 THEN firm END) AS dlr_ct_tot_2hr,
        COUNT(DISTINCT CASE WHEN quote_hour_EST = hour_EST THEN firm END) AS dlr_ct_tot_1hr
    FROM cte_historic_quotes_cum
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
        FROM cte_historic_quotes_cum
        GROUP BY cusip, firm, quote_date_EST, hour_EST
    ) AS sub
    WHERE side_count = 2
    GROUP BY cusip, quote_date_EST, hour_EST
),
cte_markets AS (
    SELECT cusip, quote_date_EST, hour_EST, COUNT(*) AS markets
    FROM (
        SELECT cusip, email_id, quote_date_EST, hour_EST,
            CASE
                WHEN SUM(CASE WHEN side = 'ask' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END +
                CASE
                    WHEN SUM(CASE WHEN side = 'bid' THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS ctmarket
        FROM cte_historic_quotes_cum
        GROUP BY cusip, email_id, quote_date_EST, hour_EST
    ) AS sub
    GROUP BY cusip, quote_date_EST, hour_EST
),
cte_dealeragg AS (
    SELECT
        cusip,
        AVG(firmquotecount) AS avg_firmquotecount,
        quote_date_EST,
        hour_EST
    FROM (
        SELECT
            cusip, firm, COUNT(*) AS firmquotecount,
            quote_date_EST, hour_EST
        FROM cte_historic_quotes_cum
        GROUP BY cusip, firm, quote_date_EST, hour_EST
    ) AS sub
    GROUP BY cusip, quote_date_EST, hour_EST
),
cte_firstbidask AS (
    SELECT
        a.cusip, a.first_bid_firm, a.first_bid_time,
        b.first_ask_firm, b.first_ask_time,
        a.quote_date_EST, a.hour_EST
    FROM (
        SELECT cusip, FIRST(firm) AS first_bid_firm, FIRST(quote_timestamp_UTC) AS first_bid_time,
               quote_date_EST, hour_EST
        FROM cte_historic_quotes_cum
        WHERE side = 'bid'
        GROUP BY cusip, quote_date_EST, hour_EST
    ) AS a
    JOIN (
        SELECT cusip, FIRST(firm) AS first_ask_firm, FIRST(quote_timestamp_UTC) AS first_ask_time,
               quote_date_EST, hour_EST
        FROM cte_historic_quotes_cum
        WHERE side = 'ask'
        GROUP BY cusip, quote_date_EST, hour_EST
    ) AS b
    ON a.cusip = b.cusip
    AND a.quote_date_EST = b.quote_date_EST
    AND a.hour_EST = b.hour_EST
),
cte_yesterday as (
    select
        cusip,
        stack_size as yest_dlr_ct
    from 
        refined_us_corporates.vw_aggr_quotes_historic_EOD
    where 
        quote_date_EST=default.prior_business_day_EST_offset_6hr() --and hour_EST=18
)
SELECT
    max(quote_identity) as max_quote_identity,
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
    LAST(case when quote_convention_flag='spread' then 'IG' else 'HY' end) AS quality_flag,
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
    CASE WHEN COALESCE(FIRST(fba.first_ask_time), '2049-01-01') >= COALESCE(FIRST(fba.first_bid_time), '2049-01-01') THEN FIRST(fba.first_bid_firm) ELSE FIRST(fba.first_ask_firm) END AS first_quote_firm,
    
    LAST(hive_metastore.default.get_issue_desc(main.cusip)) AS issue_desc,
    LAST(issue_name) AS issue_name,
    LAST(current_coupon) AS current_coupon,
    LAST(payment_category) as payment_category,
    last(default.get_bond_type(is_144a,is_regs,is_registered)) as bond_type,
    date_diff(current_date(), last(issue_date)) as pricing_period,
    last(yest.yest_dlr_ct) as eod_dlr_ct


FROM cte_historic_quotes_cum main
JOIN cte_bid_ask_counts bac ON main.cusip = bac.cusip AND main.quote_date_EST = bac.quote_date_EST AND main.hour_EST = bac.hour_EST
LEFT JOIN cte_bid_ask_firms ba ON main.cusip = ba.cusip AND main.quote_date_EST = ba.quote_date_EST AND main.hour_EST = ba.hour_EST
LEFT JOIN cte_markets mkt ON main.cusip = mkt.cusip AND main.quote_date_EST = mkt.quote_date_EST AND main.hour_EST = mkt.hour_EST
LEFT JOIN cte_dealeragg da ON main.cusip = da.cusip AND main.quote_date_EST = da.quote_date_EST AND main.hour_EST = da.hour_EST
LEFT JOIN cte_firstbidask fba ON main.cusip = fba.cusip AND main.quote_date_EST = fba.quote_date_EST AND main.hour_EST = fba.hour_EST
left join cte_yesterday yest on main.cusip=yest.cusip
GROUP BY main.cusip, main.quote_date_EST, main.hour_EST, bac.num_bids, bac.num_ask, bac.dlr_ct_bid, bac.dlr_ct_ask, bac.dlr_ct_tot, 
bac.dlr_ct_tot_6hr,bac.dlr_ct_tot_3hr,bac.dlr_ct_tot_2hr,bac.dlr_ct_tot_1hr,
ba.dlr_ct_bid_ask, bac.dlr_size_ct_bid, bac.dlr_size_ct_ask, bac.dlr_size_ct_tot
ORDER BY main.quote_date_EST, main.hour_EST;

