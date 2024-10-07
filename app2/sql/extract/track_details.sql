{% set config = {
    "extract_type": "incremental",
    "source_table_name": "track_details"
} %}

SELECT
    artists,
    available_markets,
    disc_number,
    duration_ms,
    explicit,
    href,
    id,
    is_local,
    name,
    popularity,
    preview_url,
    track_number,
    type,
    uri,
    -- Access the correct columns for album data
    "album.album_type" AS "album.album_type",
    "album.artists" AS "album.artists",
    "album.available_markets" AS "album.available_markets",
    "album.external_urls.spotify" AS "album.external_urls.spotify",
    "album.href" AS "album.href",
    "album.id" AS "album.id",
    "album.images" AS "album.images",
    "album.name" AS "album.name",
    "album.release_date" AS "album.release_date",
    "album.release_date_precision" AS "album.release_date_precision",
    "album.total_tracks" AS "album.total_tracks",
    "album.type" AS "album.type",
    "album.uri" AS "album.uri",
    "external_ids.isrc" AS "external_ids.isrc",
    "external_urls.spotify" AS "external_urls.spotify"

FROM
    {{ config["source_table_name"] }} AS t
{% if config["extract_type"] == "incremental" %}
WHERE
    id IN ({{ new_ids | join(', ') }})  -- Ensure new_ids are properly formatted
{% else %}
-- Full extract; no WHERE clause needed
{% endif %}
