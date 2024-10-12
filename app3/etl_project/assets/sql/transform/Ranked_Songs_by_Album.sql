  SELECT
        album,
        artist,
        ROW_NUMBER() OVER (PARTITION BY album ORDER BY popularity DESC) AS rank,
        popularity,
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
        extracted_at   -- Using CURRENT_TIMESTAMP to get the current time
    FROM
        Track_Features_Details_Baseline
    WHERE
        release_date IS NOT NULL  -- Ensure there's a valid release date
