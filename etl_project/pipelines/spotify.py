from dotenv import load_dotenv
import pandas as pd
import os
from etl_project.assets.assets import extract_album_track_data, extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist, extract_album_tracks, extract_audio_features, extract_track, extract_album_tracks_features, extract_album_popularity 
from etl_project.assets.assets import transform_album_info, tansform_features_track_popularity
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float


if __name__=='__main__':
    # load environmment variables
    load_dotenv()

    #client for pipeline logger
    pipeline_logging = PipelineLogging(
        pipeline_name='spotify', log_folder_path='etl_project/logs'
    ) 

    pipeline_logging.logger.info("Starting pipeline run")
    pipeline_logging.logger.info("Getting pipeline environment variables")
    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME =os.environ.get("DATABASE_NAME")
    PORT =os.environ.get("PORT")
    
    pipeline_logging.logger.info("Setting up Spotify API Client")
    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)

    pipeline_logging.logger.info("Extracting details from Spotify API")
    
    pipeline_logging.logger.info("********** Getting New Releases **********")
    pipeline_logging.logger.info("New releases")
    df_new_releases = extract_new_releases(spotify_api_client)
 
    #2 transformation techniques select certain attributes such as album name, album id, release_date, total_tracks, and etc
    #filter and rename select columns from dataframe
    select_album_info = transform_album_info(df_new_releases)

    pipeline_logging.logger.info("********** Here are the top 3 Releases **********")
    pipeline_logging.logger.info(select_album_info.head(3))


    album_track_data = extract_album_track_data(spotify_api_client, select_album_info)

    df_features = extract_album_tracks_features(spotify_api_client, album_track_data)

    df_track_popularity = extract_album_popularity(spotify_api_client, album_track_data)

    print("********** Get information about the album track **********")

    pipeline_logging.logger.info("Merging features and popularity")
    tansform_features_track_popularity(df_features=df_features, df_track_popularity=df_track_popularity)

    # ********* This is too see the dumps for these files if needed ***********
    # csv_file_path = 'features.csv'
    # df_features.to_csv(csv_file_path,index=False)
    #csv_file_path = 'track_popularity.csv'
    #df_track_popularity.to_csv(csv_file_path,index=False)


    # print("finish colleting track info and now combining the files")
    # # Transform - merge the two tables (one for audio features, one for track details including popularity)
    
    
    # This transformation technique extracts the artist's name from the first element in the list of dictionaries. It checks if x is not empty before trying to access it
    

    
     #have not used this function yet
    # df_categories = extract_categories(spotify_api_client)
    # #print("get list of catagories used to tag items in Spotify on")
    # #print(df_categories)

    # ##### Can get Top 10 tracks for Each Artist ####

    # result = extract_search_for_artist(spotify_api_client,'Imagine Dragons')
    # artist_id = result["id"]
    # songs = extract_songs_by_artist(spotify_api_client, artist_id)
    # df_songs = pd.json_normalize(songs)
    # #print("songs by artists")
    # #print(df_songs)
    # #print("Top 10 tracks for Imagine Dragons")
    # #for idx, song in enumerate(songs):
    #     #print(f"{idx + 1}. {song['name']}")
    
    
    pipeline_logging.logger.info("creating postgres client")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database_name=DATABASE_NAME,
        port=PORT,
    )

    metadata=MetaData()

    pipeline_logging.logger.info("Connection created")
    table = Table(
        'spotify', metadata,
        Column('artist_id', Integer, primary_key=True),
        Column('artist_name', String),
    )
    
    postgresql_client.create_table(metadata)

    pipeline_logging.logger.info("Table Created Successfully.")