from dotenv import load_dotenv
import os
from etl_project.assets.assets import extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist
from etl_project.connectors.postgresql import PostgreSqlClient
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
    

    df_new_releases=extract_new_releases(spotify_api_client)
    

    result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    
    artist_id = result["id"]
    songs = extract_songs_by_artist(spotify_api_client, artist_id)

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