select
    id as payment_id,
    orderid as order_id,
    paymentmethod,
    status,
    amount/100 as amount,
    created

from {{ source('stripe', 'stripe_payments') }}
