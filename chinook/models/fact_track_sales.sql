with stg_trackinvoiceline as (
    select t.*,il.invoiceid ,il.quantity,LEFT(CAST(i.invoicedate AS TIMESTAMP), 10) AS invoicedate
    from {{ source('chinook','invoiceline')}} il 
     join {{ source('chinook','Track')}} t  on t.trackid =il.trackid
     join {{ source('chinook','invoice')}} i on i.invoiceid = il.invoiceid 
),
stg_date as (
    SELECT *from {{ source('conformed','DateDimension')}}
)

SELECT {{ dbt_utils.generate_surrogate_key(['t.invoiceid']) }} as invoicekey, 
        {{ dbt_utils.generate_surrogate_key(['t.trackid']) }} as trackkey,
        d.DateKey as invoicedatekey, t.invoicedate,
        t.trackid,t.invoiceid,t.UnitPrice,
       SUM(t.Quantity) AS Quantity, 
       SUM(t.Quantity * t.UnitPrice) AS Soldamount
FROM stg_trackinvoiceline t
left join stg_date d on t.invoicedate = d.date
GROUP BY t.UnitPrice,t.trackid,t.invoiceid,d.DateKey,t.invoicedate
