import os
from dotenv import load_dotenv
from etl_project.assets.assets import extract_categories, extract_new_releases, extract_search_for_artist
import pytest
import pandas as pd
from etl_project.connectors.spotify_api import SpotifyApiClient


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

    spotify_client = SpotifyApiClient(client_id=client_id, client_secret=client_secret)

    df = extract_new_releases(spotify_api_client=spotify_client)
    assert len(df.columns) == 13