import requests
import json 
from dotenv import load_dotenv
import os

load_dotenv()

headers = { 'X-Auth-Token':  os.environ.get("API_TOKEN")} # Aqui se almacena el API_TOKEN del usuario

uri = 'https://api.football-data.org/v4/competitions/CL'

competitions_parameters = 'https://api.football-data.org/v4/competitions'

parameters_response = requests.get(url=competitions_parameters, headers=headers) # Recuests para sacar los parámetros
parameter_data = parameters_response.json()

competitions_full = parameter_data["competitions"] # Extrae todas las competencias

competitions_codes = [competition["code"] for competition in competitions_full] # Extrae todos los códigos de todas las competencias

for code in competitions_codes:
    competitions_response = requests.get(url=f'https://api.football-data.org/v4/competitions/{code}', headers=headers)
    competition_data = parameters_response.json()
    
    with open(f"../data/temp_raw/competition_{code}.json", "w", encoding="UTF-8") as competition_json:
        json.dump(competition_data, competition_json, indent=4)
        
        
# Creamos el archivo json en donde se van a almacenar el archivo con los parámetros de las competencias
# with open("../data/temp_raw/competitions_parameters.json", "w", encoding="utf-8") as info:
#     json.dump(data["competitions"]["code"], info, indent=4)
    
# with open ("../data/temp_raw/competitions_parameters.json", "r") as competitions:
#     for competition in json.load(competitions):
#         print(competition["code"])
    
# with open("../data/temp_raw/competitions_parameters.json", "r") as worker:
#     for i in json.load(worker)["competitions"]:


        
    

# response = requests.get(uri, headers=headers)

"""
Falta el análisis de cómo funcionan los archivos de la API.
(¿qué se extrae?, ¿dónde consigo los ids?, ¿qué información es util?, etc)
"""

# with open("../data/raw/matches_data_raw.json", "w") as f:
#     for match in response.json()["matches"]:
#         json.dump(match, f, indent=4)

# with open("../data/temp_raw/prueba.json", "w", encoding="utf-8") as p:
#     json.dump(response.json(), p, indent=4)