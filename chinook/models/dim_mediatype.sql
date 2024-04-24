with stg_mediatype as (
    select * from {{ source('chinook','mediatype') }}
),
stg_track as ( 
    select * from {{ source('chinook','Track') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['m.MediaTypeId']) }} as mediatypekey,  
    m.MediaTypeId,m.name as mediatype_name,t.trackid
from stg_mediatype m
join stg_track t ON m.mediatypeid = t.mediatypeid


