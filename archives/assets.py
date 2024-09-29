from etl_project.spotify_api import SpotifyApiClient
import requests
from requests import post,get
import json
import pandas as pd

url = 'https://api.spotify.com/v1/'

def extract_categories(spotify_api_client: SpotifyApiClient):
    headers = spotify_api_client.get_auth_header(spotify_api_client.token)
    query ='browse/categories'
    query_url=f'{url}{query}'
    print(query_url)
    result = get(query_url, headers=headers)

    # if(result.status_code == 200):
    #     print(result.json())
    json_result = json.loads(result.content)['categories']['items']
    df_json_result = pd.json_normalize(json_result)
    print(df_json_result.columns)

    print(df_json_result['name'])
    return df_json_result['name']