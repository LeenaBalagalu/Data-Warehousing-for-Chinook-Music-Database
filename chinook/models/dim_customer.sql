select {{ dbt_utils.generate_surrogate_key(['CustomerId']) }} 
       as customerkey,CustomerId, concat(firstname ,', ' , Lastname) as Customer_name, Company as Company_name,
        Address as Customer_Address, City as Customer_city,
        concat(City ,', ' ,COALESCE(State, 'N/A'),',',Country) as Maps_address,
        Country as Customer_Country, PostalCode as Customer_Postalcode,
        COALESCE(State, 'N/A') as Customer_State, phone as customer_phone, fax as customer_fax, email as customer_Email,SupportRepId
from {{ source('chinook','customer')}}