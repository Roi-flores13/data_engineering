import requests
import json 
from dotenv import load_dotenv
import os
import time

class api_manager:
    
    def __init__(self):
        load_dotenv()
        self.headers = { 'X-Auth-Token':  os.environ.get("API_TOKEN")} # Aqui se almacena el API_TOKEN del usuario
        
    def extract_api_data(self, temp_path="data/temp_raw") -> None:
        
        print("Iniciando extracción")
        
        competitions_parameters = 'https://api.football-data.org/v4/competitions'

        parameters_response = requests.get(url=competitions_parameters, headers=self.headers) # Recuests para sacar los parámetros
        parameter_data = parameters_response.json()

        competitions_full = parameter_data["competitions"] # Extrae todas las competencias

        competitions_id = [competition["id"] for competition in competitions_full if competition["id"] not in [2000, 2018, 2152]] # Extrae todos los ids para las competencias

        start = time.time()
            
        different_suffixes = ["matches", "standings"]

        for suffix in different_suffixes:
            
            for id in competitions_id:
                
                match_response = requests.get(url=f'https://api.football-data.org/v4/competitions/{id}/{suffix}', headers=self.headers)
                match_data = match_response.json()
                
                with open(f"data/temp_raw/{suffix}_{id}.json", "w", encoding="UTF-8") as data_json:
                    json.dump(match_data, data_json, indent=4)
                    
                time.sleep(5.5)
                
        finish = time.time()
        
        print(f"Archivos bajados en {round(finish-start, 2)} segundos")
      
    @staticmethod          
    def api_data_eraser(file_path="data/temp_raw") -> None:
            
        for file in os.listdir(file_path):
            temp_file_path = os.path.join(file_path, file)
            os.remove(temp_file_path)
                