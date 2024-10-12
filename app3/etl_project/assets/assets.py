from requests import get
import json
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData
from sqlalchemy.engine import URL, Engine
from jinja2 import Template



base_url = 'https://api.spotify.com/v1/'


# https://developer.spotify.com/documentation/web-api/reference/get-categories
def extract_categories(spotify_api_client: SpotifyApiClient):
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    query ='browse/categories'
    query_url=f'{base_url}{query}'

    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)['categories']['items']
    
    # converting to data frames
    df_json_result = pd.json_normalize(json_result)
    df_json_selected = df_json_result[['name','id']]
    return df_json_selected


# https://developer.spotify.com/documentation/web-api/reference/get-new-releases
def extract_new_releases(spotify_api_client: SpotifyApiClient, numberofreleases):
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    query=f'browse/new-releases?limit={numberofreleases}'
    query_url=f'{base_url}{query}'
   
    response = get(query_url,headers=headers)
    json_result = json.loads(response.content)['albums']['items']

    #conveting the values of album items into data frames
    df = pd.json_normalize(json_result)
    return df

# Get spotify catalog information about an album's track
# https://developer.spotify.com/documentation/web-api/reference/get-an-albums-tracks
def extract_album_tracks(spotify_api_client: SpotifyApiClient, album_id):
    query = f"albums/{album_id}/tracks"
    query_url= f'{base_url}{query}'

    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)['items']
    
    return (json_result)


# Extract audio features per track
# https://developer.spotify.com/documentation/web-api/reference/get-audio-features
# per spotify, there maybe an old style, which prevents it from working
def extract_audio_features(spotify_api_client: SpotifyApiClient, track_id):
    query = f"audio-features/{track_id}"
    query_url= f'{base_url}{query}'

    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)

    json_result = json.loads(result.content)

    return (json_result)


# Extract detailed track info
# https://developer.spotify.com/documentation/web-api/reference/get-track
def extract_track(spotify_api_client: SpotifyApiClient, track_id):
    query = f"tracks/{track_id}"
    query_url= f'{base_url}{query}'

    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)
    
    json_result = json.loads(result.content)
    
    return (json_result)


# Search artist id
# https://developer.spotify.com/documentation/web-api/reference/search
def extract_search_for_artist(spotify_api_client: SpotifyApiClient, artist_name): 
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    query = f"search/?q={artist_name}&type=artist&limit=1"
    query_url = f'{base_url}{query}'

    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)['artists']['items']
    
    if len(json_result) == 0:
        print("no artist with this name exists...")
        return None
    return json_result[0]


# https://developer.spotify.com/documentation/web-api/reference/get-an-artists-top-tracks
# Get artist top tracks
def extract_songs_by_artist(spotify_api_client: SpotifyApiClient,artist_id):
    query = f"artists/{artist_id}/top-tracks?market=US"
    query_url= f'{base_url}{query}'

    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)
    
    json_result = json.loads(result.content)['tracks']
    
    return (json_result)


#extracting the tracks data of each album
def extract_album_track_data(spotify_api_client, select_album_info):
    track_data = []
    
    for album_info in select_album_info['album_id']:
        track_data.extend(extract_album_tracks(spotify_api_client, album_id = album_info))

    return track_data


#extracting the features of each track in the album
def extract_album_tracks_features(spotify_api_client, album_track_data):
    features=[]
    for track in album_track_data:
        #Fetch Audio features
        get_features = extract_audio_features(spotify_api_client, track['id'])
        features.append(get_features)

    features = pd.json_normalize(features)

    return features


#extracts the popularity of the album
def extract_album_popularity(spotify_api_client, album_track_data):
    track_popularity = []
    
    for track in album_track_data:
        get_popularity = extract_track(spotify_api_client, track['id'])
        track_popularity.append(get_popularity)
    
    track_popularity = pd.json_normalize(track_popularity)
    
    return track_popularity


#filter and rename select columns from dataframe
def transform_album_info(df, source_table_name, postgresql_client: PostgreSqlClient):
    album_info_list = []
    print("getting source album ids")
    existing_album_ids = [
        row["album_id"]  # Using 'id' as the unique identifier
        for row in postgresql_client.engine.execute(
            f"SELECT DISTINCT album_id FROM {source_table_name} WHERE album_id IS NOT NULL"
        ).fetchall()
    ]
    print("getting source album ids", existing_album_ids)

    print(df)
    source_album_ids = df["id"].tolist()
    print("getting existing album ids", existing_album_ids)

    new_album_ids = [id for id in source_album_ids if id not in existing_album_ids] 
    print("getting new album ids", new_album_ids)

    if not new_album_ids:
        print("No new albums found for ELT.")
        return []
    
    new_album_info = df[df['id'].isin(new_album_ids)]

    # Iterate through the filtered DataFrame
    for index, album_info in new_album_info.iterrows():
        album_info_dict = {
            'album_name': album_info['name'],
            'album_name': album_info['name'],
            'album_id': album_info['id'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'artist_name': album_info['artists'][0]['name'],
            'artist_id': album_info['artists'][0]['id']
        }
        album_info_list.append(album_info_dict)
        
        df_select_album_info = pd.json_normalize(album_info_list)
    
    return df_select_album_info


def transform_features_track_popularity(df_features, df_track_popularity):
    df_merged = pd.merge(left=df_features, right = df_track_popularity, on = ["id"])
    df_merged['artist_name'] = df_merged['album.artists'].apply(lambda x: x[0]['name'] if x else None)
    
    # this transformation technique renames two of the columns
    df_merged = df_merged.rename(columns={"artist_name": "artist", "album.name": "album", "id": "track_id", "album.release_date": "release_date", "name": "song_name"})
   
    df_selected = df_merged[
        ["album", 
        "artist",
        "song_name",
        "release_date",
        "track_id",          
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
    #df_selected = df_selected.sort_values(by=['track_id'])
    df_sorted = df_selected.sort_values(by=['release_date', 'artist', 'song_name'])

    #print("******** Here are the audio characteristics of popular albums and tracks **************")
    #print(df_selected.head(50))

    # This is a dump of final file
    csv_file_path = 'selected_data.csv'
    df_sorted.to_csv(csv_file_path, index=False)
    return df_sorted


""" Some transformation techniques - lower the song_name, rounding the decimals to 2 units, finding the least popular album, ranking based on popularity"""
def transform_techniques(df_selected):
    df_selected['song_name'] = df_selected['song_name'].str.lower()
    print(df_selected)
    df_selected ['speechiness'] = df_selected['speechiness'].round(2)
    df_speach = df_selected[['speechiness']]
    print(df_speach)
    least_popular = df_selected.loc[df_selected['popularity'].idxmin()]
    print("least_popular", least_popular)
    df_selected['max_rank'] = df_selected['popularity'].rank(method='max')
    print(df_selected)
    pass

# This function loads the curated dataframe data to postgresql
def load(
        df: pd.DataFrame,
        postgresql_client: PostgreSqlClient,
        table: Table,
        metadata: MetaData,
        load_method: str="upsert"
) -> None:
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
