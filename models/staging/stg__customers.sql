with stage_customer as (
  select
    customer_id,
    customer_name,
    customer_email as email,
    customer_address as address,
    payment_method

  from {{ source('raw_src', 'customers') }}
)
select * from stage_customer
