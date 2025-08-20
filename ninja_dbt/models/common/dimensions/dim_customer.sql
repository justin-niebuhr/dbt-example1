-- models/dim_customer.sql

{{
  config(
    materialized='table',
    unique_key='customer_id'
  )
}}

with source as (
    select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  AS customer_id
    , CAST(customer_id AS VARCHAR(255)) AS SOURCE_CUSTOMER_NUMBER
    , CONVERT_TIMEZONE(TO_CHAR(current_timestamp(), 'TZH:TZM'), 'GMT', created_at::TIMESTAMP_NTZ) AS CREATED_DATETIME_GMT
	, CONVERT_TIMEZONE(TO_CHAR(current_timestamp(), 'TZH:TZM'), created_at::TIMESTAMP_NTZ) AS CREATED_DATETIME_LOCAL
    , first_name
    , last_name
    , email 
    , row_number() over (partition by customer_id order by dbt_valid_from ) as version
    , case when dbt_valid_to = to_date('9999-12-31') then true else false end as is_current
    , updated_at
    , dbt_valid_from	
    , dbt_valid_to
    from {{ ref('snapshots','snapshot_customers') }}
)

select * from source
