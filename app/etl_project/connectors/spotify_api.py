import requests
import base64
from requests import post, get
import json

class SpotifyApiClient:
    def __init__(self, client_id: str, client_secret: str):

        #setting the token
        auth_string = f"{client_id}:{client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        
        #creates the token once hte previous token is expired.
        result = post(url, headers=headers, data = data)
        json_result = json.loads(result.content)
        print(json_result)
        token = json_result["access_token"]
        self.token = token
    
    def get_token(self):
        #returning the created token
        return self.token
    
    def get_auth_header(self,token):
        #creating the header for authentication
        return{"Authorization": "Bearer " + token}
