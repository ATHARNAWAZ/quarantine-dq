{% macro quarantine_test(model, column_name, test_name, severity='warn') %}
{#
  quarantine_test — routes dbt generic test failures to Quarantine DLQ
  instead of failing the entire test run.

  Usage in schema.yml:
    models:
      - name: orders
        columns:
          - name: order_id
            tests:
              - quarantine_test:
                  test_name: not_null
                  severity: warn
              - quarantine_test:
                  test_name: unique
                  severity: warn

  This macro wraps a generic test and writes failing rows to a
  quarantine_failures table in the target schema instead of raising.
  Full DLQ integration: run `quarantine inspect --pipeline dbt` after tests.
#}

{% set test_sql %}
  {% if test_name == 'not_null' %}
    select * from {{ model }} where {{ column_name }} is null
  {% elif test_name == 'unique' %}
    select {{ column_name }}, count(*) as cnt
    from {{ model }}
    group by {{ column_name }}
    having count(*) > 1
  {% elif test_name == 'accepted_values' %}
    select * from {{ model }}
    where {{ column_name }} not in ({{ kwargs.get('values', []) | join(', ') }})
  {% else %}
    select * from {{ model }} where false  -- unknown test, no-op
  {% endif %}
{% endset %}

{% set quarantine_table = target.schema ~ '.quarantine_failures' %}

{% if execute %}
  {% set results = run_query(test_sql) %}
  {% if results.rows | length > 0 %}
    {% set insert_sql %}
      create table if not exists {{ quarantine_table }} (
        id varchar,
        pipeline_name varchar,
        stage_name varchar,
        run_id varchar,
        record_data text,
        failure_type varchar,
        failure_reason text,
        created_at timestamp
      );

      insert into {{ quarantine_table }}
      select
        gen_random_uuid()::varchar as id,
        'dbt' as pipeline_name,
        '{{ model }}' as stage_name,
        '{{ invocation_id }}' as run_id,
        to_json(t) as record_data,
        '{{ test_name }}_violation' as failure_type,
        'dbt test {{ test_name }} failed on column {{ column_name }}' as failure_reason,
        current_timestamp as created_at
      from ({{ test_sql }}) t
    {% endset %}
    {% if severity == 'error' %}
      {{ exceptions.raise_compiler_error("Quarantine: " ~ test_name ~ " failure on " ~ model ~ "." ~ column_name) }}
    {% else %}
      {% do run_query(insert_sql) %}
      {{ log("Quarantine: " ~ results.rows | length ~ " rows quarantined from " ~ model ~ "." ~ column_name, info=True) }}
    {% endif %}
  {% endif %}
{% endif %}

select 1 where false  -- this test always "passes" — failures go to quarantine

{% endmacro %}
