-- models/state_stats.sql

{{
  config(
    materialized='incremental',
    unique_key='state'
  )
}}

SELECT
  state,
  total_births,
  avg_weight,
  total_deaths,
  total_marriages,
  total_divorces
FROM
  extreme-quasar-397510.dbt_rchester.dbt_bq_demo
