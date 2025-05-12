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

        # Usar base y schema
        cursor.execute("USE DATABASE FUTBOL_DATA")
        cursor.execute("USE SCHEMA STORAGE")

        # Listar archivos en el stage
        cursor.execute("LIST @DATALAKE_FUTBOL")
        files = cursor.fetchall()

        for file in files:
            file_path = file[0]
            print(file_path)
            file_name = file_path.split("/")[-1]

            print(f"Procesando: {file_name}")

            # Leer JSON como texto
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
        
        competition_id = raw_data_standings["competition"]["id"]
        competition_name = raw_data_standings["competition"]["name"]
        competition_code = raw_data_standings["competition"]["code"]

        # Normalizar a DataFrame
        df_standings = pd.json_normalize(table)
        
        df_replaced = df_standings.rename(columns= lambda x: x.replace(".", "_"))
        
        df_replaced["competition_id"] = competition_id
        df_replaced["competition_name"] = competition_name
        df_replaced["competition_code"] = competition_code

        # Seleccionar columnas clave
        df_final_standings = df_replaced[[
            "position", "team_name", "playedGames", "points",
            "won", "draw", "lost", "goalsFor", "goalsAgainst", "goalDifference", "competition_id",
            "competition_name", "competition_code"
        ]]

        # Guardar CSV
        output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
        output_file = f"{folder_path}" + output_name
        df_final_standings.to_csv(output_file, index=False)

        print(f"Guardado como: {output_file}")
                
    def __matches_to_csv(self, folder_path, file_path):
        
        raw_data_str_matches =self.rows[0][0]
        raw_data_matches = json.loads(raw_data_str_matches)
        
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
        
        df_renamed = df_matches.rename(columns= lambda x: x.replace(".", "_"))
                
        df_matches_final = df_renamed[["id", "matchday", "stage", "main_referee", "status", "utcDate", "area_code", "area_id", "area_name",
                                       "awayTeam_id", "awayTeam_name", "awayTeam_shortName", "awayTeam_tla", "competition_code",
                                       "competition_id", "competition_name", "competition_type", "homeTeam_id", "homeTeam_name",
                                       "homeTeam_shortName", "homeTeam_tla", "score_duration", "score_fullTime_away", "score_fullTime_home", 
                                       "score_halfTime_away", "score_halfTime_home", "score_winner", "season_currentMatchday", 
                                       "season_endDate", "season_id", "season_startDate"]]
        
        
        output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
        output_file = f"{folder_path}" + output_name
        df_matches_final.to_csv(output_file, index=False)
        
        print(f"Guardado como: {output_file}")
