{% set config = {
    "extract_type": "incremental",
--    "incremental_column": "updated_at",
    "source_table_name": "audio_features"
} %}

SELECT
    mode
FROM
    {{ config["source_table_name"] }} AS t;
