select
    artist,
    song_name,
    release_date,
    album,
    popularity,
    acousticness,
    danceability,
    updated_at
from
    spotify
order by
    popularity,
    artist,
    song_name
