from dotenv import load_dotenv
import os
import base64
from requests import post, get
import json


load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

#get access token
def get_token():
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    
    result = post(url, headers=headers, data = data)

    
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

# refresh token access token (not useable)
# https://developer.spotify.com/documentation/web-api/tutorials/refreshing-tokens
def refresh_token():

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "refresh_token",
            "refresh_token": token,
            "client_id": client_id
    }
    #refresh token type
    
    result = post(url, headers=headers, data = data)

    
    json_result = json.loads(result.content)
    refreshtoken = json_result["access_token"]
    return refreshtoken

def get_auth_header(token):
    return{"Authorization": "Bearer " + token}

# Search artist id
# https://developer.spotify.com/documentation/web-api/reference/search
def search_for_artist(token, artist_name): 
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"

    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]
    if len(json_result) == 0:
        print("no artist with this name exists...")
        return None
    return json_result[0]

# https://developer.spotify.com/documentation/web-api/reference/get-an-artists-top-tracks
def get_songs_by_artist(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?market=US"
    headers = get_auth_header(token)
    result = get(url, headers = headers)
    json_result = json.loads(result.content)["tracks"]
    return json_result

token = get_token()
print(token)
result = search_for_artist(token, "taylor")
artist_id = result["id"]
songs = get_songs_by_artist(token, artist_id)
print(songs)

for idx, song in enumerate(songs):
    print(f"{idx + 1}. {song['name']}")

