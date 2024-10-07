{% set config = {
    "extract_type": "incremental",
    "source_table_name": "audio_features"
} %}

SELECT
    danceability,
    energy,
    key,
    loudness,
    mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    type,
    id,
    uri,
    track_href,
    analysis_url,
    duration_ms,
    time_signature,
    updated_at

FROM
    {{ config["source_table_name"] }} AS t
{% if config["extract_type"] == "incremental" %}
WHERE
    id IN ({{ new_ids | join(', ') }})  -- Ensure new_ids are properly formatted
{% else %}
-- Full extract; no WHERE clause needed
{% endif %}
