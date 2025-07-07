{{ config(materialized='table', tags=['mart', 'customer_orders']) }}

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

-- Aggregate order metrics per customer
customer_order_summary as (

    select
        customer_id,
        count(distinct order_id) as total_orders,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(distinct case when status = 'completed' then order_id end) as completed_orders,
        count(distinct case when status = 'returned' then order_id end) as returned_orders,
        count(distinct case when status = 'shipped' then order_id end) as shipped_orders,
        count(distinct case when status = 'placed' then order_id end) as placed_orders,
        count(distinct case when status = 'return_pending' then order_id end) as return_pending_orders

    from orders
    group by customer_id

),

-- Aggregate payment metrics per customer
customer_payment_summary as (

    select
        orders.customer_id,
        sum(payments.amount) as total_amount_paid,
        avg(payments.amount) as avg_payment_amount,
        count(distinct payments.payment_method) as unique_payment_methods,
        sum(case when payments.payment_method = 'credit_card' then payments.amount else 0 end) as credit_card_amount,
        sum(case when payments.payment_method = 'coupon' then payments.amount else 0 end) as coupon_amount,
        sum(case when payments.payment_method = 'bank_transfer' then payments.amount else 0 end) as bank_transfer_amount,
        sum(case when payments.payment_method = 'gift_card' then payments.amount else 0 end) as gift_card_amount

    from payments
    left join orders on payments.order_id = orders.order_id
    group by orders.customer_id

),

-- Calculate customer lifetime value and recency metrics
customer_metrics as (

    select
        cos.*,
        cps.total_amount_paid as customer_lifetime_value,
        cps.avg_payment_amount,
        cps.unique_payment_methods,
        cps.credit_card_amount,
        cps.coupon_amount,
        cps.bank_transfer_amount,
        cps.gift_card_amount,
        -- Recency: days since last order
        date_diff(current_date(), cos.most_recent_order_date, day) as days_since_last_order,
        -- Frequency: orders per month since first order
        case 
            when date_diff(cos.most_recent_order_date, cos.first_order_date, day) > 0
            then cos.total_orders / (date_diff(cos.most_recent_order_date, cos.first_order_date, day) / 30.0)
            else cos.total_orders
        end as avg_orders_per_month,
        -- Customer status segmentation
        case
            when cos.total_orders = 1 then 'One-time'
            when cos.total_orders between 2 and 5 then 'Regular'
            when cos.total_orders > 5 then 'Loyal'
            else 'Unknown'
        end as customer_segment

    from customer_order_summary cos
    left join customer_payment_summary cps on cos.customer_id = cps.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        cm.total_orders,
        cm.first_order_date,
        cm.most_recent_order_date,
        cm.completed_orders,
        cm.returned_orders,
        cm.shipped_orders,
        cm.placed_orders,
        cm.return_pending_orders,
        cm.customer_lifetime_value,
        cm.avg_payment_amount,
        cm.unique_payment_methods,
        cm.credit_card_amount,
        cm.coupon_amount,
        cm.bank_transfer_amount,
        cm.gift_card_amount,
        cm.days_since_last_order,
        cm.avg_orders_per_month,
        cm.customer_segment,
        -- Calculate customer health score (0-100)
        case
            when cm.customer_lifetime_value > 100 and cm.days_since_last_order < 30 then 100
            when cm.customer_lifetime_value > 50 and cm.days_since_last_order < 60 then 80
            when cm.customer_lifetime_value > 25 and cm.days_since_last_order < 90 then 60
            when cm.customer_lifetime_value > 0 and cm.days_since_last_order < 180 then 40
            else 20
        end as customer_health_score

    from customers
    left join customer_metrics cm on customers.customer_id = cm.customer_id

)

select * from final