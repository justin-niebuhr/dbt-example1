{%- macro dbt_audit(cte_ref, created_by, updated_by, created_date, updated_date) -%}

    SELECT
        *
        , OBJECT_CONSTRUCT(
            'created_by', '{{ created_by }}'::VARCHAR ,
            'updated_by', '{{ updated_by }}'::VARCHAR ,
            'model_created_date', '{{ created_date }}'::DATE ,
            'model_updated_date', '{{ updated_date }}'::DATE 
        ) as model_metadata
      , CURRENT_TIMESTAMP()               AS dbt_updated_at
      , {% if execute %}
            {% if not flags.FULL_REFRESH and config.get('materialized') == "incremental" %}

                {%- set source_relation = adapter.get_relation(
                    database=target.database,
                    schema=this.schema,
                    identifier=this.table,
                    ) -%}

                {% if source_relation != None %}

                    {% set min_created_date %}
                        SELECT LEAST(MIN(dbt_created_at), CURRENT_TIMESTAMP()) AS min_ts
                        FROM {{ this }}
                    {% endset %}

                    {% set results = run_query(min_created_date) %}

                    '{{results.columns[0].values()[0]}}'::TIMESTAMP AS dbt_created_at

                {% else %}

                    CURRENT_TIMESTAMP()               AS dbt_created_at

                {% endif %}

            {% else %}

                CURRENT_TIMESTAMP()               AS dbt_created_at

            {% endif %}
        {% endif %}

    FROM {{ cte_ref }}

{%- endmacro -%}