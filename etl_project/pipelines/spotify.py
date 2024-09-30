from dotenv import load_dotenv
import pandas as pd
import os
from etl_project.assets.assets import extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.assets import extract_categories, extract_new_releases 
from etl_project.assets.assets import extract_search_for_artist, extract_songs_by_artist, extract_album_tracks
from etl_project.assets.assets import extract_audio_features, extract_track
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

if __name__=='__main__':
    # load environmment variables
    load_dotenv()
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")

    DB_USERNAME=os.environ.get("DB_USERNAME")
    DB_PASSWORD=os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME=os.environ.get("DATABASE_NAME")
    PORT=os.environ.get("PORT")


    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)
    
    df_categories = extract_categories(spotify_api_client)
    print("get list of catagories used to tag items in Spotify on")
    #print(df_categories)
    
    df_new_releases = extract_new_releases(spotify_api_client)
    print("get_new_releases")
    #print(df_new_releases)

    album_info_list = []

    for index, album_info in df_new_releases.iterrows():
        album_info_dict = {
            'album_name': album_info['name'],
            'album_id': album_info['id'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'artist_name': album_info['artists'][0]['name'],
            'artist_id': album_info['artists'][0]['id']
        }
        album_info_list.append(album_info_dict)
    
    print("get_album_info")
    df_album_info = pd.json_normalize(album_info_list)
    #print(df_album_info)

    #get track id
    print("get album id")
    album_id = []
    for album_info in album_info_list:
        album_id.append(album_info["album_id"])

    result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    
    print("get catalog info from album track")
    track_data = []
    for album_id in album_id:
        track_data.extend(extract_album_tracks(spotify_api_client, album_id = album_id))
   
    data = []

    print("get track details including audio features and popularity for each album")
    for track in track_data:
        #Fetch Audio features
        features = extract_audio_features(spotify_api_client, track['id'])
        #Fetch track details to get popularity
        track_popularity = extract_track(spotify_api_client, track['id'])
        track_info = track_data.copy() 
        track_info = {
            'album': track_popularity['album']['name'],
            'artist': track_popularity['artists'][0]['name'],
            'track_name': track['name'],
            'track_id': track['id'],          
            'acousticness': features['acousticness'],
            'danceability': features['danceability'],
            'energy': features['energy'],
            'instrumentalness': features['instrumentalness'],
            'liveness': features['liveness'],
            'loudness': features['loudness'],
            'speechiness': features['speechiness'],
            'tempo': features['tempo'],
            'valence': features['valence'],
            'popularity': track_popularity['popularity']  # Fetch popularity
        }
        data.append(track_info)
    print("finish colleting track info")
    df = pd.DataFrame(data)

    print("here are the audio characteristics of popular albums and tracks")
    print(df.head(50))
    
    
    artist_id = result["id"]
    songs = extract_songs_by_artist(spotify_api_client, artist_id)
    print("songs")

    print("Top 10 tracks for Imagine Dragons")
    for idx, song in enumerate(songs):
        print(f"{idx + 1}. {song['name']}")
    
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database_name=DATABASE_NAME,
        port=PORT,
    )

    metadata=MetaData()
    table = Table(
        'spotify', metadata,
        Column('artist_id', Integer, primary_key=True),
        Column('artist_name', String),
    )
    
    postgresql_client.create_table(metadata)

    print("completed")
    df_songs = pd.json_normalize(songs)
    #print("songs by artists")
    #print(df_songs)
   
   
    #print("Top 10 tracks for Imagine Dragons")
    #for idx, song in enumerate(songs):
        #print(f"{idx + 1}. {song['name']}")
