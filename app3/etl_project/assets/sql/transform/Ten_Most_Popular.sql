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
    Track_Features_Details_Baseline

ORDER BY 
    popularity DESC 

LIMIT 10
