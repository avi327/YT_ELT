import requests
import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='./.env')
api_key = os.getenv("api_key")
channel_handle ='MrBeast'
def get_channel_play_listId():
    try:
        print(f'Getting channel play list id for channel handle: {api_key}')
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
if __name__ == "__main__":
    get_channel_play_listId()    