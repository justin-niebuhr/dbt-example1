{% snapshot snapshot_payments %}

{{
    config(
        unique_key='payment_id',
        strategy='timestamp',
        updated_at='payment_date'
    )
}}

select * from {{ source('loan_source', 'payments') }}

{% endsnapshot %}