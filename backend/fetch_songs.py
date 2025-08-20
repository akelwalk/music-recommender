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
NUM_PLAYLISTS_REQUESTED = 30
NUM_SONGS_TAKEN_PER_PLAYLIST = 40  # we're pulling 100 (the max) songs per playlist, and randomly choosing a sample size of 40 from it
MAX_NUM_SONGS_PER_PLAYLIST = 10 # out of the 40 randomly sampled songs from a playlist we're choosing 10 unique songs that have audio features 
VALID_SONGS = 50 # we want to try getting 50 songs per genre
RETRY_LIMIT = 30

#TODO: didnt realize that some of the songs would conflict when writing to the db - ig for a final cleanup, i can take genres with less than 50 songs and then take one of the playlist at random and keep adding songs until the valid songs limit is reached

# track the counts
def fetch_tracks_per_genre():
    all_track_ids = set() # ensures that there arent duplicate track ids between genres
    for genre in genres.GENRES:
        track_map = {}
        prev_ids = set()  # keeps track of playlists we've already tried to extract songs from - i think its okay to have the same playlist for different genres
        db_count = get_docs_from_mongo(track_map, all_track_ids, prev_ids, genre)
        if db_count >= VALID_SONGS:
            print(f"Already have {db_count} {genre} songs in database.")
            continue
        
        playlists = get_playlists(genre)
        i = 0
        while len(track_map) < VALID_SONGS and i < RETRY_LIMIT:
            print(
            "#################################################################################################################################################################"
            )
            chosen_pl = choose_playlist(playlists, i) # randomly choose one playlist
            playlist_id = chosen_pl["id"]
            # print(f"chosen playlist: {chosen_pl['name']} id: {playlist_id}")
            tracks = get_playlist_tracks(playlist_id)  # gets (num songs taken per playlist) tracks for that playlist
            audio_features = get_audio_features(tracks)
            create_track_map(
                track_map, all_track_ids, tracks, audio_features, playlist_id
            )  # modifies the track_map

            i += 1 
            prev_ids.add(playlist_id)

        # store to mongodb
        count = store_tracks_in_mongo(track_map, genre)
        
        print(
            f"Retried {i} times out of {len(playlists)} possible retries."
        )
        print(f"Number of {genre} already in database: {db_count}.")
        print(f"Number of new tracks stored for {genre}: {count}.")
        


def remove_nones(list):
    return [p for p in list if p]

def choose_playlist(playlists, i):
    i = i % len(playlists)
    return playlists[i]

def get_playlists(genre):
    result = spotify_retry_request(
        sp.search, q=genre, limit=NUM_PLAYLISTS_REQUESTED, offset=0, type="playlist"
    )  # in case of rate limiting
    playlists = result["playlists"]["items"]  # getting the playlists
    playlists = remove_nones(playlists)  # remove none values
    random.shuffle(playlists)
    return playlists


# gets n tracks for a specific playlist id
def get_playlist_tracks(playlist_id):
    result = spotify_retry_request(
        sp.playlist_items,
        playlist_id=playlist_id,
        additional_types=["track"],
        limit=100, #requesting the first 100 tracks previously NUM_SONGS_TAKEN_PER_PLAYLIST
    )
    tracks = result["items"]
    cleaned_tracks = [track for track in tracks if track and track.get("track") and track["track"].get("id")]

    sampled_tracks = random.sample(cleaned_tracks, min(NUM_SONGS_TAKEN_PER_PLAYLIST, len(cleaned_tracks))) # returning a random sample of utilizing the first 100 tracks 
    return sampled_tracks


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
    t_ids = ",".join(track_ids)
    params = {"ids": t_ids}
    response = recco_retry_request(url=url, headers=headers, params=params)
    features = response.json()["content"]
    return features

def create_track_map(track_map, all_track_ids, tracks, audio_features, playlist_id):
    new_songs = 0
    for track, features in zip(tracks, audio_features):
        if not track or not track.get("track") or not features:
            continue

        if new_songs >= MAX_NUM_SONGS_PER_PLAYLIST or len(track_map) >= VALID_SONGS:  # once we have reached the valid songs to add we can break out of this function, we don't need to add anything else to track_map
            break

        t_id = track["track"]["id"]
        if t_id not in all_track_ids: # track_map only keeps track of tids for current genre
            track_map[t_id] = [track["track"], features, playlist_id]
            all_track_ids.add(t_id)
            new_songs += 1


def store_tracks_in_mongo(track_map, genre):
    count = 0
    for t_id, data in track_map.items():  # this only contains valid tracks and features
        # if count >= REAL_LIMIT:
        #     break
        track = data[0]  # this is already track["track"]
        features = data[1]
        playlist_id = data[2]

        if track == "": #skipping empty entries - these empty entries come from reading from the database
            continue

        doc = {
            "_id": t_id,  # track id is the main ID
            "metadata": {
                "name": track["name"],
                "artists": [artist["name"] for artist in track["artists"]],
                "album": track["album"]["name"],
                "images": track["album"].get("images", []),
                "track_link": track["external_urls"]["spotify"],
            },
            "audio_features": features,
            "playlist_id": playlist_id,
            "genre": genre,
            "emb": {"triplet64": None, "ae32": None},  # leaving blank for now
        }

        result = tracks_collection.update_one(
            {"_id": t_id}, {"$setOnInsert": doc}, upsert=True
        )  # set on insert: insert if _id doesn't exist, otherwise skip

        if (result.upserted_id is not None):  # inserted a brand new track - no previous ids matched and an upsert is a new insert
            count += 1

    return count


def get_docs_from_mongo(track_map, all_track_ids, prev_ids, genre):
    cursor = tracks_collection.find({"genre": genre})
    count = tracks_collection.count_documents({"genre": genre})
    
    for doc in cursor:
        all_track_ids.add(doc["_id"])
        prev_ids.add(doc["playlist_id"])
        track_map[doc["_id"]] = ["", "", ""] #info stored is irrelevant because we're gonna skip over these tracks, they're already in the database
    return count

# wrapper to handle spotify rate limiting; can accept any function
def spotify_retry_request(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(
                    e.headers.get("Retry-After", 180)
                )  # default to 180 sec if retry-after header is missing
                print(f"Rate limited by Spotify. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                raise e


# wrapper to handle reccobeats rate limiting
def recco_retry_request(url, headers, params):
    while True:
        response = requests.request("GET", url=url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 180))
            print(f"Rate limited by Reccobeats. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            response.raise_for_status()  # raise an error if present
            return response


# calling first function
fetch_tracks_per_genre()
client.close()  # close mongodb connection
