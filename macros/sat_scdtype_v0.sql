{%- macro snowflake__sat_scdtype2_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm=false, source_is_single_batch=false) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set ns = namespace(src_hashdiff="", hdiff_alias="") -%}

{%- if src_hashdiff is mapping and src_hashdiff is not none -%}
    {%- set ns.src_hashdiff = src_hashdiff["source_column"] -%}
    {%- set ns.hdiff_alias = src_hashdiff["alias"] -%}
{%- else -%}
    {%- set ns.src_hashdiff = src_hashdiff -%}
    {%- set ns.hdiff_alias = src_hashdiff -%}
{%- endif -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_rsrc, src_ldts, src_payload]) -%}
{%- set source_relation = ref(source_model) -%}

{# Get max(ldts) for incremental processing #}
{% if execute %}
    {%- if is_incremental() %}
        {%- set max_ldts_query = 'SELECT COALESCE(MAX(' ~ src_ldts ~ '), ' ~ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) ~ ') FROM ' ~ this ~ ' WHERE ' ~ src_ldts ~ ' < ' ~ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) -%}
        {%- set max_ldts_results = run_query(max_ldts_query) -%}
        {%- set max_ldts = max_ldts_results.columns[0].values()[0] -%}
    {%- endif %}
{% endif %}

{{ datavault4dbt.prepend_generated_by() }}

WITH
source_data AS (
    SELECT
        {{ parent_hashkey }} AS parent_hashkey,
        {{ ns.src_hashdiff }} AS {{ ns.hdiff_alias }},
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }}
    FROM {{ source_relation }}
    {%- if is_incremental() %}
    WHERE {{ src_ldts }} > '{{ max_ldts }}'
      AND {{ src_ldts }} < {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    {%- endif %}
),

{# If incremental, capture latest entry per parent in the target sat to compare #}
{%- if is_incremental() %}
latest_entries_in_sat AS (
    SELECT
        parent_hashkey,
        {{ ns.hdiff_alias }} as existing_hdiff,
        sat_valid_from,
        sat_valid_to,
        sat_is_current,
        sat_ldts,
        -- choose a unique key for the existing row (depends on your sat design)
        ROW_NUMBER() OVER (PARTITION BY parent_hashkey ORDER BY sat_valid_from DESC, sat_ldts DESC) AS rn
    FROM {{ this }}
    WHERE sat_is_current = TRUE
    QUALIFY rn = 1
),
{%- endif %}

{# Deduplicate incoming source by comparing adjacent hashdiffs for the same parent #}
deduplicated_numbered_source AS (
    SELECT
        parent_hashkey,
        {{ ns.hdiff_alias }} AS incoming_hdiff,
        {{ src_ldts }} AS incoming_ldts,
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }}
        {%- if is_incremental() %}, ROW_NUMBER() OVER (PARTITION BY parent_hashkey ORDER BY {{ src_ldts }}) as rn {%- endif %}
    FROM source_data
    QUALIFY
        CASE
            WHEN {{ ns.hdiff_alias }} = LAG({{ ns.hdiff_alias }}) OVER(PARTITION BY parent_hashkey ORDER BY {{ src_ldts }}) THEN FALSE
            ELSE TRUE
        END
),

{# Build new satellite rows to insert (action = 'insert') #}
records_to_insert AS (
    SELECT
        parent_hashkey,
        incoming_hdiff AS {{ ns.hdiff_alias }},
        {{- "\n\n    " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(source_cols)) if source_cols else " *" }},
        incoming_ldts AS sat_ldts,
        incoming_ldts AS sat_valid_from,
        {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }} AS sat_valid_to,
        TRUE AS sat_is_current,
        '{{ source_model }}' AS record_source,
        'insert' AS dv_action
    FROM deduplicated_numbered_source
    {%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM latest_entries_in_sat
        WHERE latest_entries_in_sat.parent_hashkey = deduplicated_numbered_source.parent_hashkey
          AND latest_entries_in_sat.existing_hdiff = deduplicated_numbered_source.incoming_hdiff
          AND deduplicated_numbered_source.rn = 1
    )
    {%- endif %}
),

{# Build "close" rows that tell us which existing rows must be closed when a new version appears #}
records_to_close AS (
    {%- if is_incremental() %}
    SELECT
        les.parent_hashkey,
        les.existing_hdiff AS {{ ns.hdiff_alias }},
        -- keep existing payload columns if needed; we just need keys and close info
        les.sat_ldts as existing_sat_ldts,
        -- set new valid_to to incoming_ldts for the new version (close at that timestamp)
        dns.incoming_ldts AS sat_valid_to,
        FALSE AS sat_is_current,
        '{{ source_model }}' AS record_source,
        'close' AS dv_action
    FROM latest_entries_in_sat AS les
    JOIN deduplicated_numbered_source AS dns
      ON dns.parent_hashkey = les.parent_hashkey
    WHERE dns.incoming_hdiff <> les.existing_hdiff
      AND dns.rn = 1
    {%- else %}
    SELECT NULL WHERE 1=0
    {%- endif %}
)

{# Final output: both insert instructions and close instructions. Caller should MERGE this into target sat. #}
SELECT * FROM records_to_insert
UNION ALL
SELECT * FROM records_to_close

{%- endmacro -%}
