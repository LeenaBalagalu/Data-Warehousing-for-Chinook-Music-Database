with stg_invoicelineagg as (
    select 
        il.trackid,il.invoiceid,i.customerid,
        DATE_TRUNC('month', TO_TIMESTAMP(i.invoicedate)) AS month, 
        t.name AS songname,
        SUM(il.quantity) AS monthlyquantity,
        SUM(il.quantity * il.unitprice) AS monthlysoldamount
    from {{ source('chinook','invoiceline') }} il
    join {{ source('chinook','invoice') }} i on il.invoiceid = i.invoiceid
    join {{ source('chinook','Track') }} t on il.trackid = t.trackid
    group by il.trackid, month, songname,il.invoiceid,i.customerid
)

select
 {{ dbt_utils.generate_surrogate_key(['agg.invoiceid']) }} as invoicekey, 
 {{ dbt_utils.generate_surrogate_key(['agg.trackid']) }} as trackkey,
    c.customerkey,
    d.datekey as monthkey,
    agg.songname,
    agg.monthlyquantity,
    agg.monthlysoldamount
from stg_invoicelineagg agg
left join {{ ref('dim_customer') }} c on c.customerid = agg.customerid 
left join {{ ref('dim_date') }} d on d.date = agg.month
