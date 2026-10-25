import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')
api_key = os.getenv("api_key")
channel_handle ='MrBeast'
maxResults = 50
def get_playlistId():
    try:
        url = f'https://youtube.googleapis.com/youtube/v3/channels?part=ContentDetails&forHandle={channel_handle}&key={api_key}'
        response = requests.get(url)
        data = response.json()
        #print(json.dumps(data, indent=4))
        channel_items = data["items"][0]
        channel_play_listId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_play_listId)
        return channel_play_listId
    except requests.exceptions.RequestException as e:
        raise e
def get_video_ids(playlistId):
    video_ids = []
    page_token = None
    base_url = f'https://youtube.googleapis.com/youtube/v3/playlistItems?part=ContentDetails&maxResults={maxResults}&playlistId={playlistId}&key={api_key}'
    try:
        while True:
            url = base_url
            if page_token:
                url += f'&pageToken={page_token}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e
if __name__ == "__main__":
    playlistId=get_playlistId()    
    print(get_video_ids(playlistId))