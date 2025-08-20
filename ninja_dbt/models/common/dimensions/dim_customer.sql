-- models/dim_customer.sql
{{ config(materialized="table", unique_key="customer_id") }}

with
    source as (
        select
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id,
            cast(customer_id as varchar(255)) as source_customer_number,
            convert_timezone(
                to_char(current_timestamp(), 'TZH:TZM'),
                'GMT',
                created_at::timestamp_ntz
            ) as created_datetime_gmt,
            convert_timezone(
                to_char(current_timestamp(), 'TZH:TZM'), created_at::timestamp_ntz
            ) as created_datetime_local,
            first_name,
            last_name,
            email,
            row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version,
            case
                when dbt_valid_to = to_date('9999-12-31') then true else false
            end as is_current,
            updated_at,
            dbt_valid_from,
            dbt_valid_to
        from {{ ref('DBT_JUSTINNIEBUHR_SNAPSHOTS','snapshot_customers') }}
    )

select *
from source
