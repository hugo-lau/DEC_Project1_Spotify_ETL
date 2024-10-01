from dotenv import load_dotenv
import pandas as pd
import pandas as pd
import pandas as pd
import os
from etl_project.assets.assets import extract_categories, extract_new_releases 
from etl_project.assets.assets import extract_search_for_artist, extract_songs_by_artist, extract_album_tracks
from etl_project.assets.assets import extract_audio_features, extract_track, transform_album_info
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.assets import extract_categories, extract_new_releases 
from etl_project.assets.assets import extract_search_for_artist, extract_songs_by_artist, extract_album_tracks
from etl_project.assets.assets import extract_audio_features, extract_track, transform_album_info
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

if __name__=='__main__':
    # load environmment variables
    load_dotenv()
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")

    #get spotify api
    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)


    print("********** Getting New Releases **********")
    df_new_releases = extract_new_releases(spotify_api_client)
 
    #2 transformation techniques select certain attributes such as album name, album id, release_date, total_tracks, and etc
    #filter and rename select columns from dataframe
    select_album_info = transform_album_info(df_new_releases)
    df_select_album_info = pd.json_normalize(select_album_info)

    print("********** Here are the top 3 Releases **********")
    print(df_select_album_info)

    print("********** Get information about the album track **********")
    track_data = []
    for album_info in select_album_info:
        track_data.extend(extract_album_tracks(spotify_api_client, album_id = album_info["album_id"]))
    

    features = []
    track_popularity = []
    print("get details of track and its audio features")
    for track in track_data:
        #Fetch Audio features
        get_features = extract_audio_features(spotify_api_client, track['id'])
        features.append(get_features)
        #Fetch track details to get popularity
        get_popularity = extract_track(spotify_api_client, track['id'])
        track_popularity.append(get_popularity)

    df_features = pd.json_normalize(features)
    df_track_popularity = pd.json_normalize(track_popularity)

    # ********* This is too see the dumps for these files if needed ***********
    # csv_file_path = 'features.csv'
    # df_features.to_csv(csv_file_path,index=False)
    # csv_file_path = 'track_popularity.csv'
    # df_track_popularity.to_csv(csv_file_path,index=False)

    print("finish colleting track info and now combining the files")
    # Transform - merge the two tables (one for audio features, one for track details including popularity)
    df_merged = pd.merge(left=df_features, right = df_track_popularity, on = ["id"])
    # This transformation technique extracts the artist's name from the first element in the list of dictionaries. It checks if x is not empty before trying to access it
    df_merged['artist_name'] = df_merged['album.artists'].apply(lambda x: x[0]['name'] if x else None)
    # this transformation technique renames two of the columns
    df_merged = df_merged.rename(columns={"artist_name": "artist", "album.name": "album", "id": "track id"})
    df_selected = df_merged[
        ["album", 
        "artist",
        "name",
        "track id",          
        "acousticness",
        "danceability",
        "energy",
        "instrumentalness",
        "liveness",
        "loudness",
        "speechiness",
        "tempo",
        "valence",
        "popularity"]
    ]

    print("******** Here are the audio characteristics of popular albums and tracks **************")
    print(df_selected.head(50))

    # This is a dump of final file
    csv_file_path = 'selected_data.csv'
    df_selected.to_csv(csv_file_path,index=False)

     #have not used this function yet
    df_categories = extract_categories(spotify_api_client)
    #print("get list of catagories used to tag items in Spotify on")
    #print(df_categories)

    ##### Can get Top 10 tracks for Each Artist ####

    result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    artist_id = result["id"]
    songs = extract_songs_by_artist(spotify_api_client, artist_id)
    df_songs = pd.json_normalize(songs)
    #print("songs by artists")
    #print(df_songs)
    #print("Top 10 tracks for Imagine Dragons")
    #for idx, song in enumerate(songs):
        #print(f"{idx + 1}. {song['name']}")