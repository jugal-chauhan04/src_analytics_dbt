with stage_payments as (
    select
        payment_id,
        invoice_id,
        payment_date,
        amount_paid,
        payment_method,
        payment_status

    from {{ source('raw_src', 'payments') }}
)

select * from stage_payments