from dotenv import load_dotenv
import pandas as pd
import os
from etl_project.assets.assets import extract_album_track_data, extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist, extract_album_tracks, extract_audio_features, extract_track, extract_album_tracks_features, extract_album_popularity 
from etl_project.assets.assets import transform_album_info, transform_features_track_popularity, load, transform_techniques
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, TIMESTAMP, func
import yaml
from pathlib import Path
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus



if __name__=='__main__':
    # load environmment variables
    load_dotenv()

    #get config variables
    yaml_file_path =  __file__.replace(".py",".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipline_config = yaml.safe_load(yaml_file)
            config = pipline_config.get("config")
            PIPELINE_NAME = pipline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file!"
        )
    # print(config.get("log_folder_path"))
    #client for pipeline logger
    pipeline_logging = PipelineLogging(
        pipeline_name= pipline_config.get("name"),
        log_folder_path= config.get("log_folder_path")
    ) 

    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME= os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME=os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD=os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT=os.environ.get("LOGGING_PORT")

    pipeline_logging.logger.info("Setting up client for logging metadata into postgreql")
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )
    pipeline_logging.logger.info("Setting up metadata logger")
    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME,
        postgresql_client= postgresql_logging_client,
        config=config,
    )
    try:
        metadata_logger.log()
        pipeline_logging.logger.info("Starting pipeline run")
        pipeline_logging.logger.info("Getting pipeline environment variables")
        client_id=os.environ.get('CLIENT_ID')
        client_secret=os.environ.get("CLIENT_SECRET")
        numberofreleases=os.environ.get("NUMBER_OF_RELEASES")
        
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
        df_new_releases = extract_new_releases(spotify_api_client, numberofreleases=numberofreleases)
    
        #2 transformation techniques select certain attributes such as album name, album id, release_date, total_tracks, and etc
        #filter and rename select columns from dataframe
        select_album_info = transform_album_info(df_new_releases)

        pipeline_logging.logger.info(f"********** Here are the top {numberofreleases} Releases **********")
        pipeline_logging.logger.info(select_album_info)


        album_track_data = extract_album_track_data(spotify_api_client, select_album_info)

        df_features = extract_album_tracks_features(spotify_api_client, album_track_data)

        df_track_popularity = extract_album_popularity(spotify_api_client, album_track_data)

        pipeline_logging.logger.info("Merging features and popularity")
        df = transform_features_track_popularity(df_features=df_features, df_track_popularity=df_track_popularity)
        
        
        #transform_techniques(df)

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
            'spotify', 
            metadata,
            Column("album", String),
            Column("artist", String),
            Column('song_name', String),
            Column('release_date', String),
            Column('track_id', String, primary_key=True),
            Column('acousticness', Float),
            Column('danceability', Float),
            Column('energy', Float),
            Column('instrumentalness', Float),
            Column('liveness', Float),
            Column('loudness', Float),
            Column('speechiness', Float),
            Column('tempo', Float),
            Column('valence', Float),
            Column('popularity', Float),
            Column('updated_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())
        )

        postgresql_client.create_table(metadata)



        pipeline_logging.logger.info("Table Created Successfully.")

        pipeline_logging.logger.info("********** Final data frame **********")
        pipeline_logging.logger.info(df)
        
        load(
            df=df,
            postgresql_client=postgresql_client,
            table=table,
            metadata=metadata,
            load_method="upsert",
        )

        pipeline_logging.logger.info("Loaded Successfully.")
        pipeline_logging.logger.info("loading SUCCESSFUL status to pipeline_metadata table")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. see details logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAIL, logs=pipeline_logging.get_logs()
        )

