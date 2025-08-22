{% snapshot snapshot_loan_applications %}

{{
    config(
        unique_key='APPLICATION_ID',
        strategy='timestamp',
        updated_at='UPDATED_AT'
        table_format='iceberg'
    )
}}

select * from {{ source('loan_source', 'loan_applications') }}

{% endsnapshot %}