with readings as (
    select
        city,
        reading_ts,
        temp_c,
        humidity_pct
    from {{ ref('stg_weather') }}
),

readings_with_prev as (
    select
        *,
        lag(temp_c) over (partition by city order by reading_ts) as prev_temp_c
    from readings
),

flagged as (
    select
        *,
        prev_temp_c is not null
        and abs(temp_c - prev_temp_c) > 5 as is_temp_anomaly
    from readings_with_prev
)

select
    city,
    cast(reading_ts as date) as reading_date,
    avg(temp_c) as avg_temp_c,
    min(temp_c) as min_temp_c,
    max(temp_c) as max_temp_c,
    avg(humidity_pct) as avg_humidity_pct,
    min(humidity_pct) as min_humidity_pct,
    max(humidity_pct) as max_humidity_pct,
    bool_or(is_temp_anomaly) as has_temp_anomaly,
    count(*) filter (where is_temp_anomaly) as temp_anomaly_count
from flagged
group by city, cast(reading_ts as date)
