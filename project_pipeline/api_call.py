import requests
import json 
from dotenv import load_dotenv
import os
import time

def extract_api_data() -> None:
    
    load_dotenv()

    headers = { 'X-Auth-Token':  os.environ.get("API_TOKEN")} # Aqui se almacena el API_TOKEN del usuario

    competitions_parameters = 'https://api.football-data.org/v4/competitions'

    parameters_response = requests.get(url=competitions_parameters, headers=headers) # Recuests para sacar los parámetros
    parameter_data = parameters_response.json()

    competitions_full = parameter_data["competitions"] # Extrae todas las competencias

    competitions_codes = [competition["code"] for competition in competitions_full] # Extrae todos los códigos de todas las competencias
    competitions_id = [competition["id"] for competition in competitions_full] # Extrae todos los ids para las competencias


    for code in competitions_codes:
        
        competitions_response = requests.get(url=f'https://api.football-data.org/v4/competitions/{code}', headers=headers)
        competition_data = competitions_response.json()
        
        with open(f"data/temp_raw/competition_{code}.json", "w", encoding="UTF-8") as competition_json:
            json.dump(competition_data, competition_json, indent=4)
            
        time.sleep(5)
        
    different_suffixes = ["matches", "standings"]

    for suffix in different_suffixes:
        
        for id in competitions_id:
            
            match_response = requests.get(url=f'https://api.football-data.org/v4/competitions/{id}/{suffix}', headers=headers)
            match_data = match_response.json()
            
            with open(f"data/temp_raw/{suffix}_{id}.json", "w", encoding="UTF-8") as data_json:
                json.dump(match_data, data_json, indent=4)
                
            time.sleep(5)
            
def api_data_eraser(file_path="data/temp_raw") -> None:
        
    for file in os.listdir(file_path):
        temp_file_path = os.path.join(file_path, file)
        os.remove(temp_file_path)
            