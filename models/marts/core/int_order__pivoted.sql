with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (

    {%- set payment_types = ['credit_card', 'bank_transfer', 'coupon', 'gift_card'] %}
    
    select order_id,
    {% for payment_type in payment_types -%}
    sum(case when paymentmethod = '{{ payment_type }}' then amount else 0 end) as {{ payment_type }}_amount
    {%- if not loop.last -%}
    ,
    {%- endif %}
    {% endfor -%}
    from payments
    where status = 'success'
    group by 1
    order by 1
)

select * from pivoted