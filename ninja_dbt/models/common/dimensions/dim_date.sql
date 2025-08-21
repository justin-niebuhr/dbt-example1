{{ config(materialized="table", unique_key="date_day") }}
with
    date_spine as (

        {{
            dbt_utils.date_spine(
                start_date="to_date('01/01/2017', 'mm/dd/yyyy')",
                datepart="day",
                end_date="dateadd(year, 100, current_date)",
            )
        }}

    ),

    calculated as (

        select
            date_day,
            date_day as date_actual,

            dayname(date_day) as day_name,

            date_part('month', date_day) as month_actual,
            date_part('year', date_day) as year_actual,
            date_part(quarter, date_day) as quarter_actual,

            date_part(dayofweekiso, date_day) as day_of_week,
            case
                when day_name = 'Mon' then date_day else date_trunc('week', date_day)
            end as first_day_of_week,

            week(date_day) as week_of_year,

            date_part('day', date_day) as day_of_month,

            row_number() over (
                partition by year_actual, quarter_actual order by date_day
            ) as day_of_quarter,
            row_number() over (
                partition by year_actual order by date_day
            ) as day_of_year,

            case
                when month_actual < 2 then year_actual else (year_actual + 1)
            end as fiscal_year,
            case
                when month_actual < 2
                then '4'
                when month_actual < 5
                then '1'
                when month_actual < 8
                then '2'
                when month_actual < 11
                then '3'
                else '4'
            end as fiscal_quarter,

            row_number() over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as day_of_fiscal_quarter,
            row_number() over (
                partition by fiscal_year order by date_day
            ) as day_of_fiscal_year,

            to_char(date_day, 'MMMM') as month_name,

            trunc(date_day, 'Month') as first_day_of_month,
            last_value(date_day) over (
                partition by year_actual, month_actual order by date_day
            ) as last_day_of_month,

            first_value(date_day) over (
                partition by year_actual order by date_day
            ) as first_day_of_year,
            last_value(date_day) over (
                partition by year_actual order by date_day
            ) as last_day_of_year,

            first_value(date_day) over (
                partition by year_actual, quarter_actual order by date_day
            ) as first_day_of_quarter,
            last_value(date_day) over (
                partition by year_actual, quarter_actual order by date_day
            ) as last_day_of_quarter,

            first_value(date_day) over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as first_day_of_fiscal_quarter,
            last_value(date_day) over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as last_day_of_fiscal_quarter,

            first_value(date_day) over (
                partition by fiscal_year order by date_day
            ) as first_day_of_fiscal_year,
            last_value(date_day) over (
                partition by fiscal_year order by date_day
            ) as last_day_of_fiscal_year,

            datediff('week', first_day_of_fiscal_year, date_actual)
            + 1 as week_of_fiscal_year,
            floor(
                (datediff(day, first_day_of_fiscal_quarter, date_actual) / 7)
            ) as week_of_fiscal_quarter,

            case
                when extract('month', date_day) = 1
                then 12
                else extract('month', date_day) - 1
            end as month_of_fiscal_year,

            last_value(date_day) over (
                partition by first_day_of_week order by date_day
            ) as last_day_of_week,

            (year_actual || '-Q' || extract(quarter from date_day)) as quarter_name,

            (
                fiscal_year
                || '-'
                || decode(fiscal_quarter, 1, 'Q1', 2, 'Q2', 3, 'Q3', 4, 'Q4')
            ) as fiscal_quarter_name,
            ('FY' || substr(fiscal_quarter_name, 3, 7)) as fiscal_quarter_name_fy,
            dense_rank() over (
                order by fiscal_quarter_name
            ) as fiscal_quarter_number_absolute,
            fiscal_year || '-' || monthname(date_day) as fiscal_month_name,
            ('FY' || substr(fiscal_month_name, 3, 8)) as fiscal_month_name_fy,

            (
                case
                    when month(date_day) = 1 and dayofmonth(date_day) = 1
                    then 'New Year''s Day'
                    when month(date_day) = 12 and dayofmonth(date_day) = 25
                    then 'Christmas Day'
                    when month(date_day) = 12 and dayofmonth(date_day) = 26
                    then 'Boxing Day'
                end
            )::varchar as holiday_desc,
            (case when holiday_desc is null then 0 else 1 end)::boolean as is_holiday,
            date_trunc(
                'month', last_day_of_fiscal_quarter
            ) as last_month_of_fiscal_quarter,
            iff(
                date_trunc('month', last_day_of_fiscal_quarter) = date_actual,
                true,
                false
            ) as is_first_day_of_last_month_of_fiscal_quarter,
            date_trunc('month', last_day_of_fiscal_year) as last_month_of_fiscal_year,
            iff(
                date_trunc('month', last_day_of_fiscal_year) = date_actual, true, false
            ) as is_first_day_of_last_month_of_fiscal_year,
            dateadd(
                'day', 7, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_fpa,
            dateadd(
                'day', 4, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_fpa_fifth,
            dateadd(
                'day', 3, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_fpa_fourth,
            dateadd(
                'day', 44, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_billings,
            count(date_actual) over (
                partition by first_day_of_month
            ) as days_in_month_count,
            count(date_actual) over (
                partition by fiscal_quarter_name_fy
            ) as days_in_fiscal_quarter_count,

            90 - datediff(
                day, date_actual, last_day_of_fiscal_quarter
            ) as day_of_fiscal_quarter_normalised,
            12 - floor(
                (datediff(day, date_actual, last_day_of_fiscal_quarter) / 7)
            ) as week_of_fiscal_quarter_normalised,
            case
                when week_of_fiscal_quarter_normalised < 5
                then week_of_fiscal_quarter_normalised
                when week_of_fiscal_quarter_normalised < 9
                then week_of_fiscal_quarter_normalised - 4
                else week_of_fiscal_quarter_normalised - 8
            end as week_of_month_normalised,
            365 - datediff(
                day, date_actual, last_day_of_fiscal_year
            ) as day_of_fiscal_year_normalised,
            case
                when
                    (
                        (datediff(day, date_actual, last_day_of_fiscal_quarter) - 6) % 7
                        = 0
                        or date_actual = first_day_of_fiscal_quarter
                    )
                then 1
                else 0
            end as is_first_day_of_fiscal_quarter_week,

            datediff(
                'day', date_day, last_day_of_month
            ) as days_until_last_day_of_month,

            row_number() over (
                partition by fiscal_quarter_name_fy
                order by
                    case
                        when day_of_week not in (6, 7) and is_holiday = 0 then date_day
                    end nulls last
            ) as business_day_of_quarter,
            case
                when business_day_of_quarter = 3 then 1 else 0
            end as is_third_business_day_of_fiscal_quarter,
            iff(
                date_trunc('week', date_actual)
                = date_trunc('week', dateadd('day', -7, current_date)),
                true,
                false
            ) as is_last_week_from_current_date

        from date_spine

    ),

    current_date_information as (

        select
            fiscal_year as current_fiscal_year,
            first_day_of_fiscal_year as current_first_day_of_fiscal_year,
            fiscal_quarter_name_fy as current_fiscal_quarter_name_fy,
            first_day_of_month as current_first_day_of_month,
            first_day_of_fiscal_quarter as current_first_day_of_fiscal_quarter,
            date_actual as current_date_actual,
            day_name as current_day_name,
            first_day_of_week as current_first_day_of_week,
            day_of_fiscal_quarter_normalised
            as current_day_of_fiscal_quarter_normalised,
            week_of_fiscal_quarter_normalised
            as current_week_of_fiscal_quarter_normalised,
            week_of_fiscal_quarter as current_week_of_fiscal_quarter,
            day_of_month as current_day_of_month,
            day_of_fiscal_quarter as current_day_of_fiscal_quarter,
            day_of_fiscal_year as current_day_of_fiscal_year,
            days_in_fiscal_quarter_count as current_days_in_fiscal_quarter_count

        from calculated
        where current_date = date_actual

    ),

    final as (

        select
            calculated.date_day,
            calculated.date_actual,
            calculated.day_name,
            calculated.month_actual,
            calculated.year_actual,
            calculated.quarter_actual,
            calculated.day_of_week,
            calculated.first_day_of_week,
            calculated.week_of_year,
            calculated.day_of_month,
            calculated.day_of_quarter,
            calculated.day_of_year,
            calculated.fiscal_year,
            calculated.fiscal_quarter,
            calculated.day_of_fiscal_quarter,
            calculated.day_of_fiscal_year,
            calculated.month_name,
            calculated.first_day_of_month,
            calculated.last_day_of_month,
            calculated.first_day_of_year,
            calculated.last_day_of_year,
            calculated.first_day_of_quarter,
            calculated.last_day_of_quarter,
            calculated.first_day_of_fiscal_quarter,
            calculated.last_day_of_fiscal_quarter,
            calculated.first_day_of_fiscal_year,
            calculated.last_day_of_fiscal_year,
            calculated.week_of_fiscal_year,
            calculated.week_of_fiscal_quarter,
            calculated.month_of_fiscal_year,
            calculated.last_day_of_week,
            calculated.quarter_name,
            calculated.fiscal_quarter_name,
            calculated.fiscal_quarter_name_fy,
            calculated.fiscal_quarter_number_absolute,
            calculated.fiscal_month_name,
            calculated.fiscal_month_name_fy,
            calculated.holiday_desc,
            calculated.is_holiday,
            calculated.last_month_of_fiscal_quarter,
            calculated.is_first_day_of_last_month_of_fiscal_quarter,
            calculated.last_month_of_fiscal_year,
            calculated.is_first_day_of_last_month_of_fiscal_year,
            calculated.snapshot_date_fpa,
            calculated.snapshot_date_fpa_fifth,
            calculated.snapshot_date_fpa_fourth,
            calculated.snapshot_date_billings,
            calculated.days_in_month_count,
            calculated.days_in_fiscal_quarter_count,
            calculated.week_of_month_normalised,
            calculated.day_of_fiscal_quarter_normalised,
            calculated.week_of_fiscal_quarter_normalised,
            calculated.day_of_fiscal_year_normalised,
            calculated.is_first_day_of_fiscal_quarter_week,
            calculated.days_until_last_day_of_month,
            calculated.is_third_business_day_of_fiscal_quarter,
            calculated.is_last_week_from_current_date,
            current_date_information.current_date_actual,
            current_date_information.current_day_name,
            current_date_information.current_first_day_of_week,
            current_date_information.current_day_of_fiscal_quarter_normalised,
            current_date_information.current_week_of_fiscal_quarter_normalised,
            current_date_information.current_week_of_fiscal_quarter,
            current_date_information.current_fiscal_year,
            current_date_information.current_first_day_of_fiscal_year,
            current_date_information.current_fiscal_quarter_name_fy,
            current_date_information.current_first_day_of_month,
            current_date_information.current_first_day_of_fiscal_quarter,
            current_date_information.current_days_in_fiscal_quarter_count,
            current_date_information.current_day_of_month,
            current_date_information.current_day_of_fiscal_quarter,
            current_date_information.current_day_of_fiscal_year,
            iff(
                calculated.day_of_month
                <= current_date_information.current_day_of_month,
                true,
                false
            ) as is_fiscal_month_to_date,
            iff(
                calculated.day_of_fiscal_quarter
                <= current_date_information.current_day_of_fiscal_quarter,
                true,
                false
            ) as is_fiscal_quarter_to_date,
            iff(
                calculated.day_of_fiscal_year
                <= current_date_information.current_day_of_fiscal_year,
                true,
                false
            ) as is_fiscal_year_to_date,
            datediff('days', calculated.date_actual, current_date) as fiscal_days_ago,
            datediff('week', calculated.date_actual, current_date) as fiscal_weeks_ago,
            datediff(
                'months',
                calculated.first_day_of_month,
                current_date_information.current_first_day_of_month
            ) as fiscal_months_ago,
            round(
                datediff(
                    'months',
                    calculated.first_day_of_fiscal_quarter,
                    current_date_information.current_first_day_of_fiscal_quarter
                )
                / 3,
                0
            ) as fiscal_quarters_ago,
            round(
                datediff(
                    'months',
                    calculated.first_day_of_fiscal_year,
                    current_date_information.current_first_day_of_fiscal_year
                )
                / 12,
                0
            ) as fiscal_years_ago,
            iff(calculated.date_actual = current_date, 1, 0) as is_current_date,
            iff(
                date_actual between dateadd(
                    'day', -7, current_first_day_of_fiscal_quarter
                ) and dateadd('day', -1, current_first_day_of_fiscal_quarter),
                true,
                false
            ) as is_last_week_of_prior_quarter

        from calculated
        cross join current_date_information

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@gitlabs",
            updated_by="@jniebuhr",
            created_date="2025-08-21",
            updated_date="2025-08-21",
        )
    }}
