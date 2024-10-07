from dotenv import load_dotenv
import pandas as pd
import json
import os
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, String, Table, MetaData, Column, inspect, text, Column, Float, TIMESTAMP, func, Integer, Date
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader, Template
from sql.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

from sql.assets.assets import extract_album_track_data, extract_categories, extract_new_releases, extract_search_for_artist, extract_songs_by_artist, extract_album_tracks, extract_audio_features, extract_track, extract_album_tracks_features, extract_album_popularity 
from sql.assets.assets import transform_album_info, transform_features_track_popularity, load
from sql.assets.pipeline_logging import PipelineLogging
from sql.connectors.postgresql import PostgreSqlClient
from sql.connectors.spotify_api import SpotifyApiClient

def create_track_table_if_not_exists(engine:Engine):
    track_details_metadata=MetaData()
    print("create metadata for trackfeature if necessarily")
    #pipeline_logging.logger.info("Connection created")
    track_table = Table(
        'track_details', 
        track_details_metadata,
        Column("artists", String),
        Column('available_markets', String),
        Column('disc_number', Integer),
        Column('duration_ms', Integer),
        Column('explicit', String),
        Column('href', String),
        Column('id', String, primary_key=True),        
        Column('is_local', String),
        Column('name', String),
        Column('popularity', Integer),
        Column('preview_url', String),
        Column('track_number', Integer),
        Column('type', String),
        Column('uri', String),
        Column('album.album_type', String),
        Column('album.artists', String),
        Column('album.available_markets', String),
        Column('album.external_urls.spotify', String),
        Column('album.href', String),
        Column('album.id', String),
        Column('album.images', String),
        Column('album.name', String),
        Column('album.release_date', Date),
        Column('album.release_date_precision', String),
        Column('album.total_tracks', String),
        Column('album.type', String),
        Column('album.uri', String),
        Column('external_ids.isrc', String),
        Column('external_urls.spotify', String),                
    )
    track_details_metadata.create_all(engine)

def create_audio_table_if_not_exists(engine:Engine):
    audio_features_metadata=MetaData()
    print("create metatable for audiotable if necessarily")
    #pipeline_logging.logger.info("Connection created")
    audio_table = Table(
        'audio_features', 
        audio_features_metadata,
        Column("danceability", Float),
        Column('energy', Float),
        Column('key', Float),
        Column('loudness', Float),
        Column('mode', Float),
        Column('speechiness', Float),
        Column('acousticness', Float),        
        Column('instrumentalness', Float),
        Column('liveness', Float),
        Column('valence', Float),
        Column('tempo', Float),
        Column('type', String),
        Column('id', String, primary_key=True),
        Column('uri', String),
        Column('track_href', String),
        Column('analysis_url', String),
        Column('duration_ms', Integer),
        Column('time_signature', Float),
        Column('updated_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())              
    )
    audio_features_metadata.create_all(engine)

def extract(
    sql_template: Template,
    source_engine: Engine,
    target_engine: Engine,
) -> list[dict]:

    extract_type = sql_template.make_module().config.get("extract_type")

    if extract_type == "full":
        print("we are doing full extract")
        sql = sql_template.render()
        return [dict(row) for row in source_engine.execute(sql).all()]
    elif extract_type == "incremental":
        print("We are doing incremental extract")
        source_table_name = sql_template.make_module().config.get("source_table_name")

        # Fetch existing IDs from the target table
        existing_ids = {
            row["id"]  # Use 'id' as the unique identifier
            for row in target_engine.execute(
                f"SELECT DISTINCT id FROM {source_table_name} WHERE id IS NOT NULL"
            ).fetchall()
        }
        
        # Fetch all IDs from the source table
        source_ids = [
            row["id"]  # Using 'id' as the unique identifier
            for row in source_engine.execute(
                f"SELECT DISTINCT id FROM {source_table_name} WHERE id IS NOT NULL"
            ).fetchall()
        ]

        # Filter to find only new IDs
        new_ids = [id_ for id_ in source_ids if id_ not in existing_ids]  # Find new IDs
        new_ids = [f"'{id_}'" for id_ in new_ids]  # Quote the string IDs
        
        if not new_ids:
            print("No new IDs found for extraction.")
            return []

        # Render SQL to include only new IDs
        sql = sql_template.render(new_ids=new_ids)
        #print("Generated SQL for extraction", sql)

        return [dict(row) for row in source_engine.execute(sql).all()]

    else:
        raise Exception(
            f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
        )

    # elif extract_type == "incremental":
    #     # if target table exists :
    #     print("we are doing incremental extract")
    #     source_table_name = sql_template.make_module().config.get("source_table_name")
    #     if inspect(target_engine).has_table(source_table_name):
    #         incremental_column = sql_template.make_module().config.get(
    #             "incremental_column"
    #         )
    #         sql_result = [
    #             dict(row)
    #             for row in target_engine.execute(
    #                 f"select max({incremental_column}) as incremental_value from {source_table_name}"
    #             ).all()
    #         ]
    #         incremental_value = sql_result[0].get("incremental_value")

    #         sql = sql_template.render(
    #             is_incremental=True, incremental_value=incremental_value
    #         )
    #         print("Generated SQL for extraction", sql)
    #     else:
    #         sql = sql_template.render(is_incremental=False)

    #     return [dict(row) for row in source_engine.execute(sql).all()]
    # else:
    #     raise Exception(
    #         f"Extract type {extract_type} is not supported. Please use either 'full' or 'incremental' extract type."
    #     )

def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect()  # get target table schemas into metadata object
    return metadata


def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [
        Column(column.name, column.type, primary_key=column.primary_key)
        for column in existing_table.columns
    ]
    new_table = Table(table_name, new_metadata, *columns)
    new_metadata.create_all(bind=engine)
    return new_metadata


def load_rawdata(
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

def load(data: list[dict], table_name: str, engine: Engine, source_metadata: MetaData):
    target_metadata = _create_table(
        table_name=table_name, metadata=source_metadata, engine=engine
    )
    table = target_metadata.tables[table_name]
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns},
    )
    engine.execute(upsert_statement)

def transform(engine: Engine, sql_template: Template, table_name: str):
    # Drop the table if it exists
    drop_sql = f"DROP TABLE IF EXISTS {table_name};"
    engine.execute(drop_sql)

    # Create the table with the SQL template
    create_sql = f"""
    CREATE TABLE {table_name} AS (
        {sql_template.render()}
    )
    """
    engine.execute(create_sql)

# def prepare_before_load(data):
#     transformed_data = []
#     for row in data:
#         # Check if track_id is present and valid
#         if 'track_id' not in row or row['track_id'] is None:
#             # Generate or retrieve a valid track_id
#             row['track_id'] = generate_unique_track_id()  # You need to define this function
#         transformed_data.append(row)
#     return transformed_data


if __name__ == "__main__":
    load_dotenv()

    SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
    SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
    SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
    SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
    SOURCE_PORT = os.environ.get("SOURCE_PORT")

    source_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=SOURCE_DB_USERNAME,
        password=SOURCE_DB_PASSWORD,
        host=SOURCE_SERVER_NAME,
        port=SOURCE_PORT,
        database=SOURCE_DATABASE_NAME,
    )
    source_engine = create_engine(source_connection_url)

    client_id=os.environ.get('CLIENT_ID')
    client_secret=os.environ.get("CLIENT_SECRET")
    numberofreleases=os.environ.get("NUMBER_OF_RELEASES")
    
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME =os.environ.get("DATABASE_NAME")
    PORT =os.environ.get("PORT")
    

    TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT = os.environ.get("TARGET_PORT")

    target_connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=TARGET_DB_USERNAME,
        password=TARGET_DB_PASSWORD,
        host=TARGET_SERVER_NAME,
        port=TARGET_PORT,
        database=TARGET_DATABASE_NAME,
    )
    target_engine = create_engine(target_connection_url)

    #pipeline_logging.logger.info("Setting up Spotify API Client")
    spotify_api_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)
    df_new_releases = extract_new_releases(spotify_api_client, numberofreleases=numberofreleases)

    #2 transformation techniques select certain attributes such as album name, album id, release_date, total_tracks, and etc
    #filter and rename select columns from dataframe
    select_album_info = transform_album_info(df_new_releases)
    print("Here are new releases extracted", select_album_info)
    album_track_data = extract_album_track_data(spotify_api_client, select_album_info)
    print("get features")
    df_features = extract_album_tracks_features(spotify_api_client, album_track_data)
    # print("get track details")
    df_track_popularity = extract_album_popularity(spotify_api_client, album_track_data)

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database_name=DATABASE_NAME,
        port=PORT,
    )
    audio_features_metadata=MetaData()
    print("load audio_features into table")
    #pipeline_logging.logger.info("Connection created")
    audio_table = Table(
        'audio_features', 
        audio_features_metadata,
        Column("danceability", Float),
        Column('energy', Float),
        Column('key', Float),
        Column('loudness', Float),
        Column('mode', Float),
        Column('speechiness', Float),
        Column('acousticness', Float),        
        Column('instrumentalness', Float),
        Column('liveness', Float),
        Column('valence', Float),
        Column('tempo', Float),
        Column('type', String),
        Column('id', String, primary_key=True),
        Column('uri', String),
        Column('track_href', String),
        Column('analysis_url', String),
        Column('duration_ms', Integer),
        Column('time_signature', Float),
        Column('updated_at', TIMESTAMP, server_default=func.now(), onupdate=func.now())
    )
    postgresql_client.create_table(audio_features_metadata)

    # # # print("Columns in df_features:", df_features.columns)

    # # csv_file_path = 'features.csv'
    # # df_features.to_csv(csv_file_path,index=False)

    # # # Define the expected columns based on your database schema
    # # expected_columns = [
    # #     "danceability", "energy", "key", "loudness", "mode",
    # #     "speechiness", "acousticness", "instrumentalness", 
    # #     "liveness", "valence", "tempo", "type", 
    # #     "id", "uri", "track_href", "analysis_url", 
    # #     "duration_ms", "time_signature", "updated_at"
    # # ]

    # # # Check if all expected columns are present in the DataFrame
    # # missing_columns = [col for col in expected_columns if col not in df_features.columns]
    # # if missing_columns:
    # #     raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

    # print("DataFrame structure is valid.")
    load_rawdata(
        df=df_features,
        postgresql_client=postgresql_client,
        table=audio_table,
        metadata=audio_features_metadata,
        load_method="upsert",
    )

    # print("Columns in df_track:", df_track_popularity.columns)
    # csv_file_path = 'trackdetails.csv'
    # df_track_popularity.to_csv(csv_file_path,index=False)
    
    ## Prepare to load data into sql ***
    ## Extract the "name' key from the dictionaries within the list" ***
    df_track_popularity['artists'] = df_track_popularity['artists'].apply(lambda artist_list: [artist['name'] for artist in artist_list])
    df_track_popularity['album.artists'] = df_track_popularity['album.artists'].apply(lambda x: x[0]['name'] if x else None)
    #normalizing the list into json to help with increase querying capabilities
    df_track_popularity['available_markets'] = df_track_popularity['available_markets'].apply(json.dumps)
    df_track_popularity['album.available_markets'] = df_track_popularity['album.available_markets'].apply(json.dumps)

    track_details_metadata=MetaData()
    print("load track details into table")
    #pipeline_logging.logger.info("Connection created")
    track_table = Table(
        'track_details', 
        track_details_metadata,
        Column("artists", String),
        Column('available_markets', String),
        Column('disc_number', Integer),
        Column('duration_ms', Integer),
        Column('explicit', String),
        Column('href', String),
        Column('id', String, primary_key=True),        
        Column('is_local', String),
        Column('name', String),
        Column('popularity', Integer),
        Column('preview_url', String),
        Column('track_number', Integer),
        Column('type', String),
        Column('uri', String),
        Column('album.album_type', String),
        Column('album.artists', String),
        Column('album.available_markets', String),
        Column('album.external_urls.spotify', String),
        Column('album.href', String),
        Column('album.id', String),
        Column('album.images', String),
        Column('album.name', String),
        Column('album.release_date', Date),
        Column('album.release_date_precision', String),
        Column('album.total_tracks', String),
        Column('album.type', String),
        Column('album.uri', String),
        Column('external_ids.isrc', String),
        Column('external_urls.spotify', String),                
    )
    postgresql_client.create_table(track_details_metadata)

    load_rawdata(
        df=df_track_popularity,
        postgresql_client=postgresql_client,
        table=track_table,
        metadata=track_details_metadata,
        load_method="upsert",
    )

    environment = Environment(loader=FileSystemLoader("sql/extract"))
    # Assuming you have a specific SQL template name to run
    audio_features_sql_template_name = "audio_features.sql"  # Replace with your actual template name

    # Load the template from the environment
    audio_features_sql_template = environment.get_template(audio_features_sql_template_name)
    # print(type(audio_feature_sql_template))
    # try:
    #     rendered_sql = audio_feature_sql_template.render()
    #     print("Rendered SQL:", rendered_sql)
    # except Exception as e:
    #     print("Error rendering template:", e)
    # # Step 1: Define a basic SQL template
    # template_str = """
    # SELECT 
    #     id, 
    #     danceability, 
    #     energy, 
    #     mode, 
    #     acousticness 
    # FROM 
    #     audio_features;
    # """

    # # Step 2: Create the Jinja template
    # template = Template(template_str)

    # # Step 3: Render the template (no context needed for this simple example)
    # rendered = template.render()
    # results = postgresql_client.cursor().execute(rendered).fetchall()

    # # Print the results
    # print("Query Results:", results)

    # # Print the rendered SQL to check it
    # print("Rendered SQL:", rendered)

    # # Retrieve the source table name from the template configuration
    # print("table name = template")
    audio_features_table_name = audio_features_sql_template.make_module().config.get("source_table_name")


    # Extract data using the SQL template
    print("attempting extraction of audio features")
    create_audio_table_if_not_exists(target_engine)
    audio_feature_data = extract(
        sql_template=audio_features_sql_template,
        source_engine=source_engine,
        target_engine=target_engine,
    )
    # Retrieve source metadata
    # audio_feature_source_metadata = get_schema_metadata(engine=source_engine)
    # df_data=pd.json_normalize(audio_feature_data)
    # csv_file_path = 'extract.csv'
    # df_data.to_csv(csv_file_path,index=False)

    # Load the extracted data into the target engine
# Check if data is not empty before loading it into the target engine
    if audio_feature_data:  # This checks if 'data' is not empty
        print("Loading audio data into target database")
        load(
            data=audio_feature_data,
            table_name=audio_features_table_name,
            engine=target_engine,
            source_metadata=audio_features_metadata,
        )
    else:
        print("No audio data to load into the target database.")

    # Assuming you have a specific SQL template name to run
    track_details_sql_template_name = "track_details.sql"  # Replace with your actual template name

    # Load the track details template from the environment
    track_details_sql_template = environment.get_template(track_details_sql_template_name)

    track_details_table_name = track_details_sql_template.make_module().config.get("source_table_name")


    # Extract data using the SQL template
    print("attempting extraction of track details")
    create_track_table_if_not_exists(target_engine)
    track_details_data = extract(
        sql_template=track_details_sql_template,
        source_engine=source_engine,
        target_engine=target_engine,
    )
    # Retrieve source metadata
    track_details_source_metadata = get_schema_metadata(engine=source_engine)
    # df_track_data=pd.json_normalize(track_details_data)
    # csv_file_path = 'track_extract.csv'
    # df_track_data.to_csv(csv_file_path,index=False)

    # Load the extracted data into the target engine
# Check if data is not empty before loading it into the target engine
    if track_details_data:  # This checks if 'data' is not empty
        print("Loading track details data into target database")
        load(
            data=track_details_data,
            table_name=track_details_table_name,
            engine=target_engine,
            source_metadata=track_details_source_metadata,
        )
    else:
        print("No track details data to load into the target database.")

    # extract_audiofeatures_sql_template = environment.get_template(
    #     f"{extract_audiofeatures_table}.sql"
    # )
    # testr = environment.get_template(
    #     f"audio_features.sql"
    # )

    # # Define the context
    # context = {
    #     'incremental_value': 0.5,  # Example numeric value
    #     'is_incremental': True,     # or False, depending on your logic
    #     # Add other necessary context variables
    # }

    # testrender = testr.render(context)
    # print("Final Rendered SQL:", testrender)



#     for sql_path in environment.list_templates():
#         sql_template = environment.get_template(sql_path)

#         table_name = sql_template.make_module().config.get("source_table_name")
#         data = extract(
#             sql_template=sql_template,
#             source_engine=source_engine,
#             target_engine=target_engine,
#         )
#         source_metadata = get_schema_metadata(engine=source_engine)

#         # Transform the data to ensure track_id is populated


#         # Now, load the prepared data into the target database


    transform_environment = Environment(loader=FileSystemLoader("sql/transform"))

    modifiedspotify_table = "modifiedspotify"
    modifiedspotify_sql_template = transform_environment.get_template(
        f"{modifiedspotify_table}.sql"
    )
    transform(
        engine=target_engine,
        sql_template=modifiedspotify_sql_template,
        table_name=modifiedspotify_table,
    )
#     get_extracted_album = "getextractedalbums"
#     get_extracted_album_template = transform_environment.get_template(
#         f"{get_extracted_album}.sql")
#     transform(
#         engine=target_engine,
#         sql_template=get_extracted_album_template,
#         table_name=get_extracted_album,
#     )
#     count_album = "count"
#     count_album_template = transform_environment.get_template(
#         f"{count_album}.sql")
#     transform(
#         engine=target_engine,
#         sql_template=count_album_template,
#         table_name=count_album,
#     )
    
#    # get album_id 
#     #with source_engine.connect() as connection:
#     # Execute a query to get distinct album names
#     #result = connection.execute(text("SELECT DISTINCT album FROM spotify"))  # replace 'your_table_name' with the actual table name
#     #extracted_unique_album = result.fetchall()
    
#     #print (extracted_unique_album)
#     #unique_albums = [album[0] for album in extracted_unique_album]
#     #placeholders = ', '.join('?' for _ in unique_albums)
