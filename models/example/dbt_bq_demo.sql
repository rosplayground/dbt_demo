{{ config(materialized='table') }}

WITH state_births AS (
  SELECT
    state,
    COUNT(*) as total_births,
    AVG(weight_pounds) as avg_weight
  FROM
    `bigquery-public-data.samples.natality`
  WHERE
    year >= 2000
    AND weight_pounds > 0
  GROUP BY
    state
),
state_deaths AS (
  SELECT
    state,
    COUNT(*) as total_deaths
  FROM
    `bigquery-public-data.samples.natality`
  WHERE
    year >= 2000
    AND weight_pounds > 0
    AND mother_age >= 35
  GROUP BY
    state
),
state_marriages AS (
  SELECT
    state,
    COUNT(*) as total_marriages
  FROM
    `bigquery-public-data.samples.natality`
  WHERE
    year >= 2000
    AND weight_pounds > 0
    AND father_age >= 35
  GROUP BY
    state
),
state_divorces AS (
  SELECT
    state,
    COUNT(*) as total_divorces
  FROM
    `bigquery-public-data.samples.natality`
  WHERE
    year >= 2000
    AND weight_pounds > 0
    AND mother_age <= 25
  GROUP BY
    state
)
SELECT
  state_births.state,
  state_births.total_births,
  state_births.avg_weight,
  state_deaths.total_deaths,
  state_marriages.total_marriages,
  state_divorces.total_divorces
FROM
  state_births
LEFT JOIN
  state_deaths
ON
  state_births.state = state_deaths.state
LEFT JOIN
  state_marriages
ON
  state_births.state = state_marriages.state
LEFT JOIN
  state_divorces
ON
  state_births.state = state_divorces.state
ORDER BY
  state_births.total_births DESC
LIMIT
  10

