SELECT
    ship_name,
    MAX(time_ts) AS max_ts
FROM silver.ship_positions
GROUP BY ship_name
