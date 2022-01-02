with orders as (
    
    select *
    from {{ ref('stg_orders') }}
),

payment as (
    
    select *
    from {{ ref('stg_payments') }}
),

customers as (
    
    select *
    from {{ ref('stg_customers') }}
),

payment_aggrigate as (

    select 
        order_id, 
        max(created_at) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
        from payment

        where status <> 'fail'
        group by 1
),

customer_orders as (
    select 
        customers.customer_id
        , min(orders.order_date) as first_order_date
        , max(orders.order_date) as most_recent_order_date
        , count(orders.order_id) as number_of_orders
        from customers 

        left join orders on orders.customer_id = customers.customer_id 
        group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        payment_aggrigate.total_amount_paid,
        payment_aggrigate.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
        from orders

        left join payment_aggrigate
                on orders.order_id = payment_aggrigate.order_id
        left join customers
                on orders.customer_id = customers.customer_id 
),

paid_orders_self as (

    select
        t1.order_id,
        sum(t2.total_amount_paid) as clv_bad
        from paid_orders t1
        left join paid_orders t2 on t1.customer_id = t2.customer_id 
                and t1.order_id >= t2.order_id
        group by 1
        order by 1
),

final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        case when customer_orders.first_order_date = paid_orders.order_placed_at
        then 'new'
        else 'return' end as nvsr,
        paid_orders_self.clv_bad as customer_lifetime_value,
        customer_orders.first_order_date as fdos
        from paid_orders
        left join customer_orders on paid_orders.customer_id=customer_orders.customer_id
        left join paid_orders_self on paid_orders_self.order_id = paid_orders.order_id
        order by order_id
)

select * from final
