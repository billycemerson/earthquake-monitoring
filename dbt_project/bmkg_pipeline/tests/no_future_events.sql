-- Test: tidak ada event di masa depan (kemungkinan error parsing datetime)
select event_id, event_datetime
from {{ ref('stg_earthquake') }}
where event_datetime > current_timestamp + interval '1 hour'
