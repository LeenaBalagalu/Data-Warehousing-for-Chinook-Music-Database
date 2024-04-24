with f_trackinventoryanalysis as (
    select * from {{ ref('fact_track_inventory_analysis') }}
),
d_album as (
    select * from {{ ref('dim_album') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
),
d_mediatype as (
    select * from {{ ref('dim_mediatype') }}
)
select  
f.artistid,f.albumname,f.totaltracks,f.AverageTrackLength,f.totalstorage
from f_trackinventoryanalysis f
left join d_album al on al.albumkey = f.albumkey
left join d_track t on t.trackkey = f.trackkey
left join d_mediatype m on m.mediatypekey = f.mediatypekey