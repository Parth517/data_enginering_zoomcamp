select
    cast(VendorID as int64) as vendor_id,
    cast(RatecodeID as int64) as rate_code_id,
    cast(PULocationID as int64) as pu_location_id,
    cast(DOLocationID as int64) as do_location_id,

    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    store_and_fwd_flag,
    cast(passenger_count as int64) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    1 as trip_type, --yellow taxis can only be street hailed(trip_type=1)

    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    0 as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as int64) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from
    {{ source('raw_data','yellow_taxi_data_2019_2020') }}

where VendorID is not null
