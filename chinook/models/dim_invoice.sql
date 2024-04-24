select {{ dbt_utils.generate_surrogate_key(['InvoiceId']) }} as invoicekey, 
        {{ dbt_utils.generate_surrogate_key(['CustomerId']) }} as customerkey,
        InvoiceId, CustomerId, Total as Invoice_Total  ,
       CAST(Invoicedate AS TIMESTAMP) AS Invoicedate,
        concat(billingCity ,', ' ,COALESCE(billingstate, 'N/A'),',',billingCountry) as Maps_address
from {{ source('chinook','invoice')}}