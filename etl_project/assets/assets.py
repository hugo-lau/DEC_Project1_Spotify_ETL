from requests import get
import json
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyApiClient


base_url = 'https://api.spotify.com/v1/'


<<<<<<< HEAD
=======
# https://developer.spotify.com/documentation/web-api/reference/get-categories
>>>>>>> f727a94 (adding references to spotify)
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

def extract_new_releases(spotify_api_client: SpotifyApiClient):
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    query='browse/new-releases'
    query_url=f'{base_url}{query}'
    response = get(query_url,headers=headers)
    json_result = json.loads(response.content)['albums']['items']

    #conveting the values of album items into data frames
    df = pd.json_normalize(json_result)
    return df

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

def extract_songs_by_artist(spotify_api_client: SpotifyApiClient,artist_id):
    query = f"artists/{artist_id}/top-tracks?market=US"
    query_url= f'{base_url}{query}'
    print(query_url)
    headers = spotify_api_client.get_auth_header(spotify_api_client.get_token())
    result = get(query_url, headers = headers)
    json_result = json.loads(result.content)['tracks']
    return (json_result)
