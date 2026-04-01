{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns'
  )
}}

/*
  fact_earthquake — Mart layer siap pakai untuk analitik & dashboard
  Incremental: hanya append event baru
*/

with stg as (

    select * from {{ ref('stg_earthquake') }}

    {% if is_incremental() %}
    where event_datetime > (select max(event_datetime) from {{ this }})
    {% endif %}

)

select
    event_id,
    event_datetime,
    cast(event_datetime as date)        as event_date,
    date_trunc('hour', event_datetime)  as event_hour,

    coordinates,
    latitude,
    longitude,

    magnitude,
    magnitude_category,
    magnitude >= 6.0                    as is_high_magnitude,
    magnitude >= 7.0                    as is_major_earthquake,

    depth_km,
    depth_category,

    wilayah,
    dirasakan,

    magnitude_valid,
    depth_valid,

    ingestion_time,
    ingestion_date,
    source_file

from stg
