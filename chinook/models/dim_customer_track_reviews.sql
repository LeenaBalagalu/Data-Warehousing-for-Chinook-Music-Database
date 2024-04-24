with stg_customer as (
    select * from {{ source('chinook', 'customer') }}
),
stg_track as (
    select * from {{ source('chinook', 'Track') }}
),
stg_customer_track_reviews as (
    select * from {{ source('chinook', 'customer_track_reviews') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['c.customerid', 't.trackid']) }}  as trackreviewkey, 
    c.customerid as customerid,
    t.trackid as trackid,
    ctr.sentiment as sentiment
from stg_customer c
join stg_customer_track_reviews ctr on c.customerid = ctr.customerid
join stg_track t on ctr.trackid = t.trackid