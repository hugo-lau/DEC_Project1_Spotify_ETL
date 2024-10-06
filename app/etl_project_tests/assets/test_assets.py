import os
from dotenv import load_dotenv
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.assets import extract_categories, extract_new_releases, extract_search_for_artist, load, transform_features_track_popularity
import pytest
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyApiClient
from sqlalchemy import Table, MetaData, Column, String, Integer


@pytest.fixture
def setup_extract():
    load_dotenv()


def test_extract_categories(setup_extract):
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')

    spotify_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)

    df = extract_categories(spotify_client)
    assert (len(df.columns)) == 2


def test_extract_new_releases(setup_extract):
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    numberofreleases = os.environ.get('NUMBER_OF_RELEASES')

    spotify_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)

    df = extract_new_releases(spotify_api_client=spotify_client, numberofreleases=numberofreleases)
    assert len(df.columns) == 13



@pytest.fixture
def setup_input_dataframes():
    df_features = pd.DataFrame({
        'id': [1, 2, 3],
        'album.name': ['Album1', 'Album2', 'Album3'],
        'album.release_date': ['2020-01-01', '2021-01-01', '2022-01-01'],
        'name': ['Song1', 'Song2', 'Song3'],
        'album.artists': [[{'name': 'Artist1'}], [{'name': 'Artist2'}], [{'name': 'Artist3'}]],
        'acousticness': [0.1, 0.2, 0.3],
        'danceability': [0.5, 0.6, 0.7],
        'energy': [0.8, 0.9, 1.0],
        'instrumentalness': [0.0, 0.0, 0.0],
        'liveness': [0.1, 0.2, 0.3],
        'loudness': [-5, -3, -1],
        'speechiness': [0.05, 0.06, 0.07],
        'tempo': [120, 130, 140],
        'valence': [0.5, 0.6, 0.7],
    })

    df_track_popularity = pd.DataFrame({
        'id': [1, 2, 3],
        'popularity': [50, 60, 70]
    })

    return df_features, df_track_popularity

def test_transform_features_track_popularity(setup_input_dataframes):
    df_features, df_track_popularity = setup_input_dataframes

    result = transform_features_track_popularity(df_features, df_track_popularity)

    # Assertions to validate the output
    assert isinstance(result, pd.DataFrame), "Output should be a DataFrame"
    assert result.shape[0] == 3, "Output DataFrame should have 3 rows"
    assert result.shape[1] == 15, "Output DataFrame should have 14 columns"
    
    # Check the renamed columns
    expected_columns = ["album", "artist", "song_name", "release_date", "track_id",
                        "acousticness", "danceability", "energy", "instrumentalness",
                        "liveness", "loudness", "speechiness", "tempo", "valence", "popularity"]
    assert list(result.columns) == expected_columns, "Columns should match expected names"

    # Check if the artist names are correctly extracted
    assert result['artist'].iloc[0] == 'Artist1', "Artist name for first track should be 'Artist1'"
    assert result['artist'].iloc[1] == 'Artist2', "Artist name for second track should be 'Artist2'"

    # Check if the popularity values are correctly merged
    assert result['popularity'].iloc[0] == 50, "Popularity for first track should be 50"
    assert result['popularity'].iloc[1] == 60, "Popularity for second track should be 60"

    # Ensure the sorting is correct
    assert result.iloc[0]['release_date'] <= result.iloc[1]['release_date'], "DataFrame should be sorted by release_date"



@pytest.fixture
def setup_transformed_dataframe():
    return pd.DataFrame(
        [
            {"id": 1, "name": "bob", "value": 100},
            {"id": 2, "name": "sam", "value": 200},
            {"id": 3, "name": "sarah", "value": 300},
        ]
    )


@pytest.fixture
def setup_postgresql_client():
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    return postgresql_client


@pytest.fixture
def setup_transformed_table_metadata():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("value", Integer),
    )
    return table_name, table, metadata


def test_load(
    setup_postgresql_client,
    setup_transformed_dataframe,
    setup_transformed_table_metadata,
):
    postgresql_client = setup_postgresql_client
    df = setup_transformed_dataframe
    table_name, table, metadata = setup_transformed_table_metadata
    postgresql_client.drop_table(table_name)  # reset
    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    assert len(postgresql_client.select_all(table=table)) == 3

    load(
        df=df,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    assert len(postgresql_client.select_all(table=table)) == 3
    #postgresql_client.drop_table(table_name)  # reset
