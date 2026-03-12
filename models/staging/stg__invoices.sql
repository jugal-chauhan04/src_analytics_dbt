with staging_invoices as(
    select 
        invoice_id,
        subscription_id,
        invoice_date,
        total_due as amount_due,
        invoice_status
        
    from {{ source('raw_src', 'invoices') }}
)

select * from staging_invoices