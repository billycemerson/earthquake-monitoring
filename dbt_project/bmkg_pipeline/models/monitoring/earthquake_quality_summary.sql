/*
  earthquake_quality_summary — Pipeline observability model

  Upgrade vs v1:
  - pipeline_status: OK / WARNING / FAILED (actionable)
  - Freshness tied ke SLA (warn >2h, fail >12h)
  - Sub-status per dimensi: freshness, anomaly, quality
  - Tracking invalid records
*/

with base as (
    select * from {{ ref('fact_earthquake') }}
),

stats as (
    select
        current_timestamp                                                   as run_time,
        count(*)                                                            as total_records,
        count(distinct event_date)                                          as distinct_days_covered,
        round(min(magnitude), 1)                                            as min_magnitude,
        round(max(magnitude), 1)                                            as max_magnitude,
        round(avg(magnitude), 2)                                            as avg_magnitude,
        sum(case when magnitude >= 6 then 1 else 0 end)                     as high_magnitude_count,
        sum(case when magnitude >= 7 then 1 else 0 end)                     as major_earthquake_count,
        max(event_datetime)                                                 as latest_event_datetime,
        round(
            extract(epoch from (current_timestamp - max(event_datetime))) / 3600, 2
        )                                                                   as freshness_lag_hours,
        sum(case when not magnitude_valid then 1 else 0 end)                as invalid_magnitude_count,
        sum(case when not depth_valid     then 1 else 0 end)                as invalid_depth_count,
        sum(case when latitude is null or longitude is null then 1 else 0 end) as unparseable_coord_count,
        max(ingestion_time)                                                 as latest_ingestion_time,
        count(distinct ingestion_date)                                      as distinct_ingestion_dates
    from base
)

select
    *,

    case
        when freshness_lag_hours > 12 then 'FAILED'
        when freshness_lag_hours > 2  then 'WARNING'
        else                               'OK'
    end as freshness_status,

    case
        when major_earthquake_count > 0 then 'WARNING'
        else                                 'OK'
    end as anomaly_status,

    case
        when invalid_magnitude_count > 0 or invalid_depth_count > 0 then 'WARNING'
        else                                                              'OK'
    end as quality_status,

    case
        when freshness_lag_hours > 12                                                  then 'FAILED'
        when freshness_lag_hours > 2 or major_earthquake_count > 0
             or invalid_magnitude_count > 0 or invalid_depth_count > 0                then 'WARNING'
        else                                                                                'OK'
    end as pipeline_status

from stats
