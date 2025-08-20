-- models/dim_customer.sql
{{ config(materialized="table", unique_key="customer_id") }}

with
    primary_source as (
        select
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id,
            cast(customer_id as varchar(255)) as source_customer_number,
            convert_timezone(
                GET_CURRENT_TIMEZONE(),
                'GMT',
                created_at::timestamp_ntz
            )::timestamp_ntz as created_datetime_gmt,
            convert_timezone(
                GET_CURRENT_TIMEZONE(), created_at::timestamp_ntz
            )::timestamp_ntz as created_datetime_local,
            first_name,
            last_name,
            email,
            row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version,
            case
                when dbt_valid_to is null then true else false
            end as is_current,
            dbt_updated_at,
            dbt_valid_from,
            case
                when dbt_valid_to is null then to_date('9999-12-31') 
            end as dbt_valid_to,
        from {{ ref('snapshot_customers') }}
    )

select *
from source
