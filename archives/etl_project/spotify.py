from dotenv import load_dotenv
import os

from spotify_api import SpotifyApiClient
from archives.assets import extract_categories

from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv


if __name__=='__main__':
    load_dotenv()
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")

    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)
    df_categories = extract_categories(spotify_api_client)

    print(df_categories)
