-- models/dim_customer.sql
{{ config(materialized="incremental", unique_key="payment_id") }}

WITH snapshot_payments AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(["payment_id"]) }} as payment_id 
        , cast(payment_id as varchar(255)) as SOURCE_PAYMENT_NUMBER
        , {{ dbt_utils.generate_surrogate_key(["loan_id"]) }} as loan_id
        , {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id 
        , payment_amount
        , payment_date
        ,{{ status_short('payments','payment_type') }} as payment_type
    FROM {{ ref('snapshot_payments') }}
    WHERE dbt_valid_to IS NULL  -- get latest version of each row
),


transformed_payments AS (
    SELECT
        payment_id 
        , SOURCE_PAYMENT_NUMBER
        , loan_id
        , customer_id
        , payment_amount
        , payment_date
        , payment_type
    FROM order_snapshot
    {% if is_incremental() %}

      WHERE payment_date >= (SELECT MAX(payment_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM transformed_payments
