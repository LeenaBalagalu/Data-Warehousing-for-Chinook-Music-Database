with f_artistreleaseanalysis as (
    select * from {{ ref('fact_artist_release_analysis') }}
),
d_artist as (
    select * from {{ ref('dim_artist') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
)
select  a.ArtistName as artist_name,t.*,f.Song_releases
from f_artistreleaseanalysis f
left join d_artist a on a.artistkey = f.artistkey 
left join d_track t on t.trackkey = f.trackkey