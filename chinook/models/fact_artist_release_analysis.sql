with stg_trackgeal as (
    select t.*, ge.Name as Genre_name, a.title, a.ArtistId  from {{ source('chinook','Track')}}  t 
    Left join   {{ source('chinook','Album')}} a on a.albumid=t.albumid
    left join  {{ source('chinook','genre')}} ge on ge.genreid= t.genreid 
),
stg_artist as (
    select * from {{ source('chinook','Artist')}}
)
select {{ dbt_utils.generate_surrogate_key(['a.ArtistId']) }} as artistkey, 
        {{ dbt_utils.generate_surrogate_key(['tgl.trackid']) }} as trackkey,
tgl.TrackId, a.ArtistId,tgl.AlbumId, tgl.Name as Song_Name, a.Name as Artist_name, tgl.Genre_name,
            case when tgl.TrackId is not null and a.ArtistId is not null then '1' else '0' end as Song_releases
from stg_artist a 
left join stg_trackgeal tgl on a.ArtistId = tgl.ArtistId