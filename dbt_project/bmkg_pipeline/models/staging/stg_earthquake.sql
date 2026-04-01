{{
  config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns'
  )
}}

/*
  stg_earthquake — Staging layer dengan deduplication & incremental load

  Upgrade vs v1:
  - Incremental: hanya proses record baru (berdasarkan ingestion_time)
  - Deduplication: ROW_NUMBER() untuk handle duplicate dari API
  - Parse koordinat → lat/lon terpisah
  - Tambah magnitude_category dan depth_category
  - Flag validasi: magnitude_valid, depth_valid
*/

with source as (

    select * from raw_earthquake

    {% if is_incremental() %}
    where ingestion_time > (select max(ingestion_time) from {{ this }})
    {% endif %}

),

deduped as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingestion_time desc
        ) as row_num

    from source

),

cleaned as (

    select
        event_id,
        cast(datetime as timestamptz)                        as event_datetime,
        coordinates,
        magnitude,
        depth_km,
        wilayah,
        dirasakan,
        ingestion_time,
        ingestion_date,
        source_file,

        -- Parse koordinat jadi lat/lon terpisah
        try_cast(split_part(coordinates, ',', 1) as double)  as latitude,
        try_cast(split_part(coordinates, ',', 2) as double)  as longitude,

        -- Kategorisasi magnitude (skala Richter)
        case
            when magnitude < 3.0 then 'micro'
            when magnitude < 4.0 then 'minor'
            when magnitude < 5.0 then 'light'
            when magnitude < 6.0 then 'moderate'
            when magnitude < 7.0 then 'strong'
            when magnitude < 8.0 then 'major'
            else                      'great'
        end as magnitude_category,

        -- Kategorisasi kedalaman
        case
            when depth_km < 70  then 'shallow'
            when depth_km < 300 then 'intermediate'
            else                     'deep'
        end as depth_category,

        -- Validasi range — di-flag, bukan di-drop
        magnitude between 0 and 10  as magnitude_valid,
        depth_km > 0                as depth_valid

    from deduped
    where row_num = 1   -- Hanya keep row pertama per event_id (dedup)

)

select * from cleaned
