with f_trackreview as (
    select * from {{ ref('fact_track_review') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_custtrackreview as (
    select * from {{ ref('dim_customer_track_reviews') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
)
select  c.*,t.*,f.likes,f.sentiment from f_trackreview f
left join d_custtrackreview ct on ct.trackreviewkey = f.customerkey and ct.trackreviewkey = f.trackkey
left join d_customer c on c.customerkey = f.customerkey
left join d_track t on t.trackkey = f.trackkey