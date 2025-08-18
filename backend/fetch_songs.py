import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient
import os
import random
import requests
import time


load_dotenv()  # load variables from .env
random.seed()  # seeding rng with the current time

# client credentials flow
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# setting up spotipy client
auth_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret
)
sp = spotipy.Spotify(auth_manager=auth_manager)

# const variables
GENRES = ["Afrobeat"]
NUM_SONGS_PER_PLAYLIST = 5  # we are currently only getting 1 playlist per genre, and this many songs per playlist (max # of songs: 100)


# need error code 429 (rate limit exceeded handling)
def get_playlists_per_genre():
    for genre in GENRES:
        result = spotify_retry_request(sp.search, q=genre, limit=10, offset=0, type="playlist") #in case of rate limiting
        playlists = result["playlists"]["items"]  # getting the playlists
        chosen_pl = choose_playlist(playlists)  # randomly choose one playlist
        tracks = get_playlist_tracks(chosen_pl["id"])  # get tracks for that playlist
        print(
            "##################################################################################################################################################################"
        )
        audio_features = get_audio_features(tracks)
        print(audio_features)
        # using playlist id and genre name, function to write the data or sumn to mongodb


# randomly chooses a single playlist given a list of playlists
def choose_playlist(playlists):
    index = random.randint(0, len(playlists) - 1)
    chosen = playlists[index]
    while not chosen:  # makes sure chosen playlist is not null
        index = random.randint(0, len(playlists) - 1)
        chosen = playlists[index]
    return chosen


# gets n tracks for a specific playlist id
def get_playlist_tracks(playlist_id):
    tracks = spotify_retry_request(sp.playlist_items, playlist_id=playlist_id, additional_types=["track"], limit=NUM_SONGS_PER_PLAYLIST)
    return tracks["items"]

# gets ids of tracks
def get_track_ids(tracks):
    ids = []
    for track in tracks:
        if track and track.get("track") and track["track"].get("id"):
            ids.append(track["track"]["id"])
        else:
            print(f"This track doesn't have an id: {track}")
    return ids

# calls recco beat's api to get audio features
def get_audio_features(tracks):
    track_ids = get_track_ids(tracks)
    features = []
    url = "https://api.reccobeats.com/v1/audio-features"
    headers = {"Accept": "application/json"}
    for id in track_ids:
        if id:
            params = {"ids": id}
            response = recco_retry_request(url=url, headers=headers, params=params) #in case of rate limiting
            features.append(response.json()["content"])
    return features

# wrapper to handle spotify rate limiting; can accept any function
def spotify_retry_request(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get('Retry-After', 5))  # default to 5 sec if retry-after header is missing
                print(f"Rate limited by Spotify. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise e

# wrapper to handle reccobeats rate limiting
def recco_retry_request(url, headers, params):
    while True:
        response = requests.request("GET", url=url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            print(f"Rate limited by Reccobeats. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            response.raise_for_status() # raise an error if present
            return response

# calling first function
get_playlists_per_genre()
