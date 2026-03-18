-- Databricks notebook source
CREATE MATERIALIZED VIEW aggr_trace_current_day TBLPROPERTIES("delta.autoOptimize.optimizeWrite"="true",
        "delta.autoOptimize.autoCompact"="true","pipelines.trigger.interval" = "15 minute")
        AS
WITH today_intervals AS (
    SELECT
        date(from_utc_timestamp(current_timestamp(), 'America/New_York')) AS trade_date,
        explode(sequence(6, LEAST(hour(from_utc_timestamp(current_timestamp(), 'America/New_York')), 18))) AS trade_hour
),
hourly_intervals AS (
    SELECT * FROM today_intervals
),
cte_trades_historic AS (
    SELECT *,
           trade_date,
           hour(from_utc_timestamp(trade_timestamp, 'America/New_York')) AS trade_data_hour
    FROM trusted_us_corporates.vw_trace_with_reference_trading_days
    WHERE date(from_utc_timestamp(trade_timestamp, 'America/New_York')) = date(from_utc_timestamp(current_timestamp(), 'America/New_York'))
),
cte_trades_cumulative AS (
    SELECT
        hi.trade_hour,
        th.*
    FROM hourly_intervals hi
    LEFT JOIN cte_trades_historic th
        ON th.trade_date = hi.trade_date
        AND th.trade_data_hour <= hi.trade_hour
),
cte_trades_with_rank AS (
    SELECT
        DENSE_RANK() OVER (PARTITION BY trade_date,trade_hour,cusip ORDER BY transaction_id DESC) AS rn,
        CASE
            WHEN ((trade_size >= 500000) AND quality_flag = 'HY') OR (trade_size >= 1000000) THEN 1
            ELSE 0
        END AS if_inst_tr,
        *
    FROM cte_trades_cumulative
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
        CASE
            WHEN trade_size >= 250000 THEN trade_size
            ELSE NULL
        END AS vol_250k,
        CASE
            WHEN trade_size >= 500000 THEN trade_size
            ELSE NULL
        END AS vol_500k,
        CASE
            WHEN trade_size >= 1000000 THEN trade_size
            ELSE NULL
        END AS vol_1mm,
        CASE
            WHEN trade_size >= 5000000 THEN trade_size
            ELSE NULL
        END AS vol_5mm,
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
    ORDER BY trade_date,trade_hour,rn
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
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN trade_flow = 'DSC' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS DSCvol,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN trade_flow = 'D2D' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS D2Dvol,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN trade_flow = 'DSA' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS DSAvol,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN trade_flow = 'DBC' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS DBCvol,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN trade_flow = 'DBA' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS DBAvol,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN ats_indicator = 'Y' THEN trade_size
                        ELSE NULL
                    END
                ),
                0
            ) AS DECIMAL
        ) AS ATSvol
    FROM cte_trades_with_rank_ordered
    GROUP BY trade_date, trade_hour, cusip
),
cte_lastInstTS AS (
    SELECT
        trade_date,
        trade_hour,
        cusip,
        LAST(trade_timestamp) AS inst_last_trade_timestamp
    FROM cte_trades_cumulative
    WHERE (trade_size >= 500000 AND quality_flag = 'HY') OR (trade_size >= 1000000)
    GROUP BY trade_date, trade_hour, cusip
),
cte_yesterday as (
    select cusip as yest_cusip,vwap as yest_vwap,vwat as yest_vwat from refined_us_corporates.aggr_trace_historic_calc where trade_date=default.prior_business_day_EST_offset_6hr() and trade_hour=18
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
        (SUM(DSCvol) - SUM(DBCvol)) / (SUM(DSCvol) + SUM(DBCvol)) * 100,
        0
    ) AS customer_flow_pct,
    ROUND((SUM(DSCvol) - SUM(DBCvol)), 0) AS customer_flow_sum,
    FIRST(i.inst_last_trade_timestamp) AS inst_last_trade_timestamp
FROM cte_trades_agg1 ag1
LEFT JOIN cte_lastInstTS i ON ag1.trade_date = i.trade_date AND ag1.trade_hour = i.trade_hour AND ag1.cusip = i.cusip
left join cte_yesterday yest on ag1.cusip = yest.yest_cusip
GROUP BY ag1.trade_date, ag1.trade_hour, ag1.cusip
ORDER BY ag1.trade_date DESC, ag1.trade_hour DESC, ag1.cusip;

