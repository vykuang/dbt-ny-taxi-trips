{{ config(
    materialized="view",
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

-- Q2: do not add deduplication step
-- with fhv_trips as (
--     select 
--         *, 
--         row_number() over (
--             partition by
--                 dispatching_base_num,
--                 pickup_datetime
--             ) as row_num
--     from {{ source("staging", "fhv_taxi_trips") }}
--     where 
--         dispatching_base_num is not null
-- )
select
    -- identifiers
    {{ dbt_utils.surrogate_key(["Affiliated_base_number", "pickup_datetime"]) }} as tripid,
    -- FHV source schema holds locID as ints
    cast(PULocationID as string) as pickup_locationid,
    cast(DOLocationID as string) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(sr_flag as string) as sr_flag,

    -- base number info
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(affiliated_base_number as string) as affiliated_base_num
-- from fhv_trips
from {{ source('staging', 'fhv_taxi_trips') }}
-- where row_num = 1
-- dbt build --m model.sql --var 'is_test_run:false'
{% if var("is_test_run", default=True) %} limit 100 {% endif %}
