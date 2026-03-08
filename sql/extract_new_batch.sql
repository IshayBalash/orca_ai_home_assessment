SELECT
    to_timestamp(time) as time,
    ship_name,
    lat,
    lon,
    sog,
    cog
FROM raw.ship_positions
WHERE to_timestamp(time) >= $start_ts
AND to_timestamp(time) < $end_ts