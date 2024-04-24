with stg_trackreview as (
    select * from {{ source('chinook','customer_track_reviews')}}
)

select {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        {{ dbt_utils.generate_surrogate_key(['trackid']) }} as trackkey, customerid, trackid , 
        case when sentiment ='like' then '1' else '0' end as likes, sentiment,
        case when sentiment ='dislike' then '1' else '0' end as dislikes
from stg_trackreview