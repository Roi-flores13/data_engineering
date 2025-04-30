import requests
import json 
from dotenv import load_dotenv
import os

load_dotenv()

uri = 'https://api.football-data.org/v4/matches'
headers = { 'X-Auth-Token':  os.environ.get("API_TOKEN")}

response = requests.get(uri, headers=headers)

with open("../data/raw/matches_data_raw.json", "w") as f:
    json.dump(response.json()["matches"], f, indent=4)