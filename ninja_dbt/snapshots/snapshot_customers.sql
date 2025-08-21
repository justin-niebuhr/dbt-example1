{% snapshot snapshot_customers %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='created_at'
    )
}}

/*
    -- Multiple changes since last snap use last one

    SELECT
    *
    FROM {{ source('loan_source', 'customers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1

    -- Duplicate loads of same data state use first

    SELECT *
    FROM {{ source('loan_source', 'customers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, updated_at ORDER BY other_load_timestamp ASC ) = 1
*/
-- Keep it simple
select * from {{ source('loan_source', 'customers') }}

{% endsnapshot %}