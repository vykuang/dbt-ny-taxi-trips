{{ config(materialized='table') }}

with fhv_taxi_trips as (
    select *, 'fhv' as service_type
    from
        {{ ref('stg_fhv_taxi_trips') }}
),
-- filter the lookup.csv before joining
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_taxi_trips.*,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
from fhv_taxi_trips
-- inner join prevents null pickup_locationid that appears in left join
inner join dim_zones as pickup_zone
on fhv_taxi_trips.pickup_locationid = cast(pickup_zone.locationid as string)
inner join dim_zones as dropoff_zone
on fhv_taxi_trips.dropoff_locationid = cast(dropoff_zone.locationid as string)