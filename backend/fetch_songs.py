import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv
import random
import requests


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
        result = sp.search(
            q=genre, limit=10, offset=0, type="playlist"
        )  # default limit returns 10
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


# gets first 100 tracks for a specific playlist id
# for each track, items and then track are the useful fields ->
def get_playlist_tracks(playlist_id):
    tracks = sp.playlist_items(
        playlist_id, additional_types=["track"], limit=NUM_SONGS_PER_PLAYLIST
    )
    return tracks["items"]


def get_track_ids(tracks):
    ids = []
    for track in tracks:
        if track and track.get("track") and track["track"].get("id"):
            ids.append(track["track"]["id"])
        else:
            print(f"This track doesn't have an id: {track}")
    return ids


def get_audio_features(tracks):
    track_ids = get_track_ids(tracks)
    features = []
    url = "https://api.reccobeats.com/v1/audio-features"
    headers = {"Accept": "application/json"}
    for id in track_ids:
        if id:
            params = {"ids": id}
            response = requests.request("GET", url, headers=headers, params=params)
            features.append(response.json()["content"])
    return features


# calling first function
get_playlists_per_genre()
