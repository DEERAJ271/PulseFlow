with source as (
    select * from {{ source('weather', 'weather_raw') }}
)

select
    city,
    cast(timestamp as timestamp) as reading_ts,
    record_type,
    cast(temperature as double) as temp_c,
    cast(humidity as double) as humidity_pct,
    cast(wind_speed as double) as wind_speed_kmh,
    cast(conditions as integer) as weather_code,
    loaded_at
from source
