with stg_track as (
    select * from {{ source('chinook','Track')}}
),
stg_genre as (
    select * from {{ source('chinook','genre')}}
),
stg_mediatype as (
    select * from {{ source('chinook','mediatype')}}
),
stg_album as (
    select * from {{ source('chinook','Album')}}
)
select {{ dbt_utils.generate_surrogate_key(['tr.TrackId']) }} 
       as trackkey, tr.TrackId, tr.Name as Song_Name, al.title as album_name ,al.ArtistId, g.Name as Genre , mt.Name as MediaType, Composer,
        Milliseconds,Bytes,UnitPrice, (Milliseconds/60000) as Minutes  
from stg_track tr
left join stg_genre g on g.GenreId=tr.GenreId
left join stg_mediatype mt on mt.MediaTypeId = tr.MediaTypeId 
left join stg_album al on al.AlbumId=tr.AlbumId