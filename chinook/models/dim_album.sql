with stg_artist as (
    select * from {{ source('chinook','Artist') }}
),
stg_album as ( 
    select * from {{ source('chinook','Album') }}
),
stg_genre as (
    select * from {{ source('chinook','genre') }}
),
stg_track as (
    select * from {{ source('chinook','Track') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['al.albumid']) }} as albumkey,  
    al.albumid,al.title as album_name, ar.artistid,ar.name as artist_name,t.name as track_name,g.name as genre_name
from stg_album al
join stg_artist ar on al.artistid = ar.artistid
join stg_track t on t.albumid = al.albumid
join stg_genre g on g.genreid = t.genreid
