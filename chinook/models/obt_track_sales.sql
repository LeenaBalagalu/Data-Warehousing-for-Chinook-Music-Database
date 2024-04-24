with f_tracksales as (
    select * from {{ ref('fact_track_sales') }}
),
d_invoice as (
    select * from {{ ref('dim_invoice') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
)
select i.*,t.*,f.quantity,f.soldamount
from f_tracksales f
left join d_invoice i on i.invoicekey = f.invoicekey
left join d_track t on t.trackkey = f.trackkey