with

source as (

    select * from {{ source('stripe', 'payment') }}

),

transformed as (

    select 

        id as payment_id,
        orderid as order_id,
        status as payment_status,
        round(amount / 100 , 2) as payment_amount_usd

    from source

)

select * from transformed