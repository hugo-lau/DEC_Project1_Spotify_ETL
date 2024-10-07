{% set config = {
    "extract_type": "full",
--    "incremental_column": "updated_at",
    "source_table_name": "spotify"
} %}

select
    track_id,
    album,
    artist,
    song_name,
    release_date,
    acousticness,
    danceability,
    energy,
    instrumentalness,
    liveness,
    loudness,
    speechiness,
    tempo,
    valence,
    popularity,
    updated_at

from
    {{ config["source_table_name"] }}

--{% if is_incremental %}
--    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
--{% endif %}