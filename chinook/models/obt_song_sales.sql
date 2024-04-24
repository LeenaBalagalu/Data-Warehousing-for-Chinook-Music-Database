with f_songsales as (
    select * from {{ ref('fact_song_sales') }}
),
d_invoice as (
    select * from {{ ref('dim_invoice') }}
),
d_track as (
    select * from {{ ref('dim_track') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select  
c.customerkey,i.invoiceid,Invoice_Total,invoicedate,c.Customer_name,
f.songname,f.monthlyquantity,f.monthlysoldamount
from f_songsales f
left join d_invoice i on i.invoicekey = f.invoicekey
left join d_track t on t.trackkey = f.trackkey
left join d_customer c on c.customerkey = f.customerkey
left join d_date d on d.datekey = f.monthkey