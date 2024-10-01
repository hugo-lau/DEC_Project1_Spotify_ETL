from requests import get
import json
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyApiClient
base_url = 'https://api.spotify.com/v1/'



### Extract ####

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
def extract_new_releases(spotify_api_client: SpotifyApiClient):
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    query='browse/new-releases?limit=3'
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
    #print(query_url)
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
    #print(query_url)
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)
    return (json_result)

# Extract detailed track info
# https://developer.spotify.com/documentation/web-api/reference/get-track
def extract_track(spotify_api_client: SpotifyApiClient, track_id):
    query = f"tracks/{track_id}"
    query_url= f'{base_url}{query}'
    #print(query_url)
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
    #print(query_url)
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)['tracks']
    return (json_result)

#filter and rename select columns from dataframe
def transform_album_info(df):
    album_info_list = []
    for index, album_info in df.iterrows():
        album_info_dict = {
            'album_name': album_info['name'],
            'album_id': album_info['id'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'artist_name': album_info['artists'][0]['name'],
            'artist_id': album_info['artists'][0]['id']
        }
        album_info_list.append(album_info_dict)
    return album_info_list
