    SELECT
        t."album.name" AS album,
        t."album.artists" AS artist,
        t.name AS song_name,
        t."album.release_date" AS release_date,
        t.id AS track_id,
        a.acousticness,
        a.danceability,
        a.energy,
        a.instrumentalness,
        a.liveness,
        a.loudness,
        a.speechiness,
        a.tempo,
        a.valence,
        t.popularity,
        a.extracted_at  -- Using CURRENT_TIMESTAMP to get the current time
    FROM
        audio_features a
    INNER JOIN
        track_details t ON a.id = t.id
    ORDER BY
        t."album.release_date",  -- Adjust this based on actual column name
        t."album.artists",       -- Adjust this based on actual column name
        t.name