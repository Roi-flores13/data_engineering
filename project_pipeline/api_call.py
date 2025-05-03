import requests
import json 
from dotenv import load_dotenv
import os

load_dotenv()

uri = 'https://api.football-data.org/v4/competitions/DED/standings'
headers = { 'X-Auth-Token':  os.environ.get("API_TOKEN")}

response = requests.get(uri, headers=headers)

"""
Falta el análisis de cómo funcionan los archivos de la API.
(¿qué se extrae?, ¿dónde consigo los ids?, ¿qué información es util?, etc)
"""

# with open("../data/raw/matches_data_raw.json", "w") as f:
#     for match in response.json()["matches"]:
#         json.dump(match, f, indent=4)

with open("../data/raw/DED.json", "w", encoding="utf-8") as p:
    json.dump(response.json(), p, indent=4)