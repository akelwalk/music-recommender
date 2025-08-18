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
import genres


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

# connect to mongo db database
uri = os.getenv("MONGODB_URL")
client = MongoClient(
    uri,
    server_api=pymongo.server_api.ServerApi(
        version="1", strict=True, deprecation_errors=True
    ),
)
db = client["musicdb"]
tracks_collection = db["tracks"]

# const variables
NUM_SONGS_REQUESTED_PER_PLAYLIST = 100  # we are currently only getting 1 playlist per genre, and this many songs per playlist (max # of songs: 100)
VALID_SONGS = 50 
# global variable
count = 0

#track the counts
def get_playlists_per_genre():
    global count
    for genre in genres.GENRES:
        count = 0 #TODO: need to make a function to get the current count of the genre already in the database - count function with optional filter argumensts
        result = spotify_retry_request(
            sp.search, q=genre, limit=10, offset=0, type="playlist"
        )  # in case of rate limiting
        playlists = result["playlists"]["items"]  # getting the playlists
        playlists = remove_nones(playlists) #remove none values
        prev_ids = "" # keeps track of playlists we've already tried to extract songs from
        retry_count = len(playlists)

        while count < VALID_SONGS and retry_count > 0:
            chosen_pl = choose_playlist(playlists, prev_ids)  # randomly choose one playlist
            playlist_id = chosen_pl["id"]
            tracks = get_playlist_tracks(playlist_id)  # get tracks for that playlist
            audio_features = get_audio_features(tracks)
            # using playlist id and genre name, function to write the data or sumn to mongodb
            count = store_tracks_in_mongo(tracks, audio_features, genre, playlist_id)

            retry_count -= 1
            prev_ids += playlist_id + " "
        
        print(
            "##################################################################################################################################################################"
        )
        print(f"Retried {len(playlists-retry_count)} times out of {len(playlists)} possible retries")
        print(f"Number of tracks stored for {genre}: {count}")

def remove_nones(list):
    new_list = []
    for l in list:
        if l:
            new_list.append(l)
    return new_list

# randomly chooses a single playlist given a list of playlists
def choose_playlist(playlists, prev_ids):
    index = random.randint(0, len(playlists) - 1)
    chosen = playlists[index]
    while chosen["id"] in prev_ids: # makes sure chosen playlist is different from previous ones
        index = random.randint(0, len(playlists) - 1)
        chosen = playlists[index]
    return chosen


# gets n tracks for a specific playlist id
def get_playlist_tracks(playlist_id):
    tracks = spotify_retry_request(
        sp.playlist_items,
        playlist_id=playlist_id,
        additional_types=["track"],
        limit=NUM_SONGS_REQUESTED_PER_PLAYLIST,
    )
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
            response = recco_retry_request(
                url=url, headers=headers, params=params
            )  # in case of rate limiting
            features.append(response.json()["content"])
    return features


# wrapper to handle spotify rate limiting; can accept any function
def spotify_retry_request(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(
                    e.headers.get("Retry-After", 5)
                )  # default to 5 sec if retry-after header is missing
                print(f"Rate limited by Spotify. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise e


# wrapper to handle reccobeats rate limiting
def recco_retry_request(url, headers, params):
    while True:
        response = requests.request("GET", url=url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited by Reccobeats. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            response.raise_for_status()  # raise an error if present
            return response


def store_tracks_in_mongo(tracks, audio_features, genre, playlist_id):
    global count
    for track, features in zip(tracks, audio_features):
        if count == VALID_SONGS:
            break
        if not track or not track.get("track") or not features:
            continue
        t = track["track"]
        doc = {
            "_id": t["id"],  # track id is the main ID
            "metadata": {
                "name": t["name"],
                "artists": [artist["name"] for artist in t["artists"]],
                "album": t["album"]["name"],
                "images": t["album"].get("images", []),
                "track_link": t["external_urls"]["spotify"]
            },
            "audio_features": features[0],
            "playlist_id": playlist_id,
            "genre": genre,
            "emb": {"triplet64": None, "ae32": None},  # leaving blank for now
        }

        result = tracks_collection.update_one(
            {"_id": t["id"]}, {"$set": doc}, upsert=True
        )  # upsert: insert if not exists, update if exists

        if result.matched_count == 0 and result.upserted_id is not None: #inserted a brand new track - no previous ids matched and an upsert is a new insert
            count += 1
    return count


# calling first function
get_playlists_per_genre()
client.close()  # close mongodb connection
