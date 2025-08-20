-- models/dim_customer.sql
{{ config(materialized="table", unique_key="customer_id") }}

with
    source as (
        select
            {{ dbt_utils.generate_surrogate_key(["application_id"]) }} as application_id
            , {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as customer_id
            , cast(application_id as varchar(255)) as source_application_number
            , application_date as  APPLICATION_SUBMIT_DATE
            , loan_amount_requested as REQUESTED_LOAN_AMOUNT
            , status as APPLICATION_STATUS
            , convert_timezone(
                GET_CURRENT_TIMEZONE(),
                'GMT',
                updated_at::timestamp_ntz
            )::timestamp_ntz as updated_at_gmt
            , convert_timezone(
                GET_CURRENT_TIMEZONE(), updated_at::timestamp_ntz
            )::timestamp_ntz as updated_at_local
            , row_number() over (
                partition by customer_id order by dbt_valid_from
            ) as version
            , case
                when dbt_valid_to is null then true else false
            end as is_current
            , dbt_updated_at
            , dbt_valid_from
            , case
                when dbt_valid_to is null then to_date('9999-12-31') 
            end as dbt_valid_to
        from {{ ref('snapshot_loan_application') }}
    )

select *
from source
