select event_id, magnitude
from {{ ref('stg_earthquake') }}
where magnitude <= 0
