-- Test: koordinat dalam bounding box Indonesia
-- Lat: -11 s/d 6, Lon: 95 s/d 141
select event_id, latitude, longitude
from {{ ref('stg_earthquake') }}
where latitude  < -11 or latitude  > 6
   or longitude <  95 or longitude > 141
