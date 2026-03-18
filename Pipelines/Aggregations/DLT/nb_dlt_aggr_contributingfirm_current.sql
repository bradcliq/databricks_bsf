-- Databricks notebook source
CREATE MATERIALIZED VIEW aggr_contributingfirm_current
TBLPROPERTIES("delta.autoOptimize.optimizeWrite"="true",
        "delta.autoOptimize.autoCompact"="true","pipelines.trigger.interval" = "10 minute")
 AS
with cte_3stack as (
select cusip from trusted_us_corporates.vw_quotes where quote_date_est=default.current_date_EST() group by cusip having count(distinct firm)>=3
)
select cusip,'all' as firm from cte_3stack
union
select distinct cusip,firm as firm from trusted_us_corporates.vw_quotes where quote_date_est=default.current_date_EST() and cusip not in (select cusip from cte_3stack)
