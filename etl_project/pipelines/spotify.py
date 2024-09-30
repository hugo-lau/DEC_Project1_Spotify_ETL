from dotenv import load_dotenv
import os
from etl_project.assets.assets import extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist
from etl_project.connectors.spotify_api import SpotifyApiClient


if __name__=='__main__':
    # load environmment variables
    load_dotenv()
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")


    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)
    
    df_categories = extract_categories(spotify_api_client)
    print("df_catagories")
    print(df_categories)
    
    df_new_releases=extract_new_releases(spotify_api_client)
    print("df_new_releases")
    print(df_new_releases)

    result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    print("result")
    print(result)
    
    artist_id = result["id"]
    songs = extract_songs_by_artist(spotify_api_client, artist_id)
    print("songs")

    print("Top 10 tracks for Imagine Dragons")
    for idx, song in enumerate(songs):
        print(f"{idx + 1}. {song['name']}")