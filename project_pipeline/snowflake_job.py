import snowflake.connector
from dotenv import load_dotenv
import os
from pathlib import Path
import subprocess
from project_pipeline.api_call import api_manager
import pandas as pd
import json
class snowflake_connector:
    def __init__(self) -> None:
        
        load_dotenv()
        
        # Extracción de las credenciales desde el .env
        PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        USER = os.getenv("SNOWFLAKE_USER")
        ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        SCHEMA = os.getenv("SCHEMA")
        DATABASE = os.getenv("DATABASE")
        
        # Conexión a Snowflake mediante las credenciales
        self.conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        database=DATABASE,
        schema=SCHEMA
        )
        
        print("conexión establecida con Snowflake")
        
    def upload_to_snowflake_datalake(self, local_folder_path="data/temp_raw", stage_name='@DATALAKE_FUTBOL', connection='my_connection', erase_api_temp=True) -> None:
        
        print("Subiendo datos")
        
        folder_path = Path(local_folder_path).resolve() # Extrae el path del archivo a subir
        folder_path = folder_path.as_posix() # Cambia las diagonales para que no haya errores

        # Armar el comando que se escribiría en la terminal para hacer el PUT en Snowflake 
        command = [
            'snowsql',
            '-c', connection,
            '-q', f"PUT file://{folder_path}/* {stage_name} AUTO_COMPRESS=FALSE"
        ]

        # Se intenta subir el archivo al DataLake de SnowFlake
        try:
            print(f"Ejecutando: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, check=True) # Se corre el comando como si fuera en la terminal
            print(f"Subido correctamente a: {stage_name}")
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print(f"Error al subir archivo a: {stage_name}")
            print(e.stderr)
        
        if erase_api_temp:
            api_manager.api_data_eraser()
        
            
    def erase_everything_from_datalake(self, stage_name="@DATALAKE_FUTBOL", connection="my_connection") -> None:
        
        # Armar el comando que va a eliminar los archivos del datalake
        command = [
            "snowsql",
            "-c", connection,
            "-q", f"REMOVE  {stage_name}"
        ]
        
        # Se intenta ejecutar el comando de borrar
        try:
            print(f"Ejecutando: {" ".join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print(f"Archivos borrados correctamente de: {stage_name}")
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print(f"Error al borrar los archivos de: {stage_name}")
            print(e.stderr)
            
            
    def extraer_tabla_desde_json_stage(self, folder_path="data/processed/") -> None:
        
        cursor = self.conn.cursor()

        # 1. Usar base y schema
        cursor.execute("USE DATABASE FUTBOL_DATA")
        cursor.execute("USE SCHEMA STORAGE")

        # 2. Listar archivos en el stage
        cursor.execute("LIST @DATALAKE_FUTBOL")
        files = cursor.fetchall()

        for file in files:
            file_path = file[0]
            print(file_path)
            file_name = file_path.split("/")[-1]

            print(f"Procesando: {file_name}")

            # 3. Leer JSON como texto (cada fila es un dict)
            cursor.execute(f"""
                SELECT $1 
                FROM @DATALAKE_FUTBOL/{file_name}
                (FILE_FORMAT => 'my_json_format')
            """)
            self.rows = cursor.fetchall()

            if not self.rows:
                print("Archivo vacío.")
                continue
            
            if "standings" in file_name:

                try:
                    self.__standings_to_csv(folder_path, file_path)
                    
                except Exception as e:
                    print(f"Error procesando {file_path}: {e}")
                    
            elif "competition" in file_name:
                
                try:
                    self.__competition_to_csv(folder_path, file_path)
                    
                except Exception as e:
                    print(f"Error procesando {file_path}: {e}")
                    
            elif "matches" in file_name:
                
                try:
                    self.__matches_to_csv(folder_path, file_path)
                    
                except Exception as e:
                    print(f"Error procesando {file_path}: {e}")
                    
                    
    def __standings_to_csv(self, folder_path, file_path) -> None:
        # Asumimos que hay un solo objeto JSON por archivo
        
        raw_data_str_standings = self.rows[0][0]  # primer dict completo del archivo
        raw_data_standings = json.loads(raw_data_str_standings)

        # Extraer standings > table
        table = raw_data_standings["standings"][0]["table"]

        # Normalizar a DataFrame
        df_standings = pd.json_normalize(table)

        # Seleccionar columnas clave
        df_final_standings = df_standings[[
            "position", "team.name", "playedGames", "points",
            "won", "draw", "lost", "goalsFor", "goalsAgainst", "goalDifference"
        ]]

        # Guardar CSV
        output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
        output_file = f"{folder_path}" + output_name
        df_final_standings.to_csv(output_file, index=False)

        print(f"Guardado como: {output_file}")
        
    
    def __competition_to_csv(self, folder_path, file_path) -> None:
        
        raw_data_str_competitions = self.rows[0][0]
        raw_data_competitions = json.loads(raw_data_str_competitions)
        
        season_table = raw_data_competitions["seasons"]
        
        # Normalizar la lista de temporadas
        df_competitions = pd.json_normalize(season_table, sep='.', max_level=1)
        
        df_final_competitions = df_competitions[["endDate", "id", "startDate", "winner.address", "winner.clubColors", "winner.founded", "winner.id",
                       "winner.name", "winner.shortName", "winner.tla", "winner.venue"]]

        # Guardar como CSV
        output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
        output_file = f"{folder_path}" + output_name
        df_final_competitions.to_csv(output_file, index=False)
        
        print(f"Guardado como: {output_file}")
        
    def __matches_to_csv(self, folder_path, file_path):
        
        raw_data_str_matches =self.rows[0][0]
        raw_data_matches =json.loads(raw_data_str_matches)
        
        match_table = raw_data_matches["matches"]
        
        main_refs = []
        
        for match in match_table:
            try:
                ref = match["referees"][0]["name"]
                main_refs.append(ref)
                
            except Exception as e:
                main_refs.append(None)
                print(f"Could not extract referee name for {file_path}: {e}")
                            
        df_matches = pd.json_normalize(match_table)
        
        df_matches["main_referee"] = main_refs
                
        df_matches_final = df_matches[["id", "matchday", "stage", "main_referee", "status", "utcDate", "area.code", "area.id", "area.name",
                                       "awayTeam.id", "awayTeam.name", "awayTeam.shortName", "awayTeam.tla", "competition.code",
                                       "competition.id", "competition.name", "competition.type", "homeTeam.id", "homeTeam.name",
                                       "homeTeam.shortName", "homeTeam.tla", "score.duration", "score.fullTime.away", "score.fullTime.home", 
                                       "score.halfTime.away", "score.halfTime.home", "score.winner", "season.currentMatchday", 
                                       "season.endDate", "season.id", "season.startDate"]]
        
        
        output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
        output_file = f"{folder_path}" + output_name
        df_matches_final.to_csv(output_file, index=False)
        
        print(f"Guardado como: {output_file}")
