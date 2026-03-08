SELECT
    ship_name,
    MAX(time_ts) AS time_ts,
    MAX_BY(lat, time_ts) AS lat,
    MAX_BY(lon, time_ts) AS lon,
    MAX_BY(sog, time_ts) AS sog,
    MAX_BY(cog, time_ts) AS cog
FROM silver.ship_positions
GROUP BY ship_name
