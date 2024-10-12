from dotenv import load_dotenv
from etl_project.connectors.spotify_api import SpotifyApiClient
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')

    yield {'client_id': client_id, 'client_secret':client_secret}

# pytest to test if the retured token is string
def test_spotify_client_get_token(setup):
    client_id = setup['client_id']
    client_secret = setup['client_secret']

    spotify_client = SpotifyApiClient(client_id=client_id, client_secret= client_secret)
    data = spotify_client.get_token()

    assert len(data) > 0
    assert type(data) == str


#pytest to check if the header returened has both bearer and token in it
def test_get_header(setup):
    client_id = setup['client_id']
    client_secret = setup['client_secret']
    
    spotify_client = SpotifyApiClient(client_id=client_id, client_secret= client_secret)
    data = spotify_client.get_token()

    header_values = spotify_client.get_auth_header(data)
    assert type(header_values)== dict
    assert header_values['Authorization'] == 'Bearer '+ data