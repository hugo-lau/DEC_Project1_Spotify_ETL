SELECT
    album,
    artist,
    song_name,
    release_date,
    track_id,
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
    extracted_at   -- Using CURRENT_TIMESTAMP to get the current time
FROM
    Ranked_Songs_by_Album
WHERE
    rank <= 3  -- Get up to 3 songs from each album
ORDER BY
    album, popularity DESC  -- Order by album and then by popularity in descending order
