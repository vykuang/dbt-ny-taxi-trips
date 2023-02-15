{{ config(
    materialized="view",
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

with indexed_trips as (
    select 
        *, 
        row_number() over (
            partition by
                PULocationID,
                DOLocationID,
                lpep_pickup_datetime,
                lpep_dropoff_datetime
            ) as row_num
    from {{ source("staging", "green_taxi_trips") }}
    where 
        lpep_pickup_datetime is not null and
        lpep_dropoff_datetime is not null and
        PULocationID is not null and
        DOLocationID is not null
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key([
        "PULocationID", 
        "DOLocationID", 
        "lpep_pickup_datetime", 
        "lpep_dropoff_datetime"]) 
    }} as tripid,
    cast(vendorid as string) as vendorid,
    cast(ratecodeid as string) as ratecodeid,
    cast(pulocationid as string) as  pickup_locationid,
    cast(dolocationid as string) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as string) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as string) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
from indexed_trips
where row_num = 1
-- from {{ source('staging','green_taxi_trips') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
