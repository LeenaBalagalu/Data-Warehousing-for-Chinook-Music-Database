with stg_artist as (
    select * from {{ source('chinook','Artist') }}
),
stg_album as ( 
    select * from {{ source('chinook','Album') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['a.artistid']) }} as artistkey,  
    a.artistid, 
    a.Name as ArtistName,al.AlbumId
from stg_artist a 
left join stg_album al on al.artistid = a.artistid 


