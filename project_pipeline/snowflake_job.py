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
        
    def upload_to_snowflake_datalake(self, local_folder_path="data/temp_raw", stage_name='@DATALAKE_FUTBOL', connection='my_connection') -> None:
        
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
            
            
    def json_to_csv(self):
        
        # cursor = self.conn.cursor()
        # cursor.execute("USE database FUTBOL_DATA")
        # cursor.execute("USE SCHEMA STORAGE")
        # cursor.execute("LIST @DATALAKE_FUTBOL")
        
        # files = cursor.fetchall()
        
        # for file in files:
        #     file_path = file[0]
            
        #     query = f"""
        #         SELECT $1 
        #         FROM @DATALAKE_FUTBOL/{file_path}
        #         (FILE_FORMAT => 'my_json_format')
        #     """
            
        #     cursor.execute(query)
        #     rows = cursor.fetchall()
            
        #     df = pd.DataFrame([row[0] for row in rows])
            
        #     nombre_salida = file_path.split("/")[-1].replace(".json", "csv")
        #     df.to_csv(nombre_salida, index=False)
            
        #     print(f"Archivo guardado como {nombre_salida}")
    
        import pandas as pd

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
            file_name = file_path.split("/")[-1]

            print(f"Procesando: {file_name}")

            # 3. Leer JSON como texto (cada fila es un dict)
            cursor.execute(f"""
                SELECT $1 
                FROM @DATALAKE_FUTBOL/{file_name}
                (FILE_FORMAT => 'my_json_format')
            """)
            rows = cursor.fetchall()

            if not rows:
                print("Archivo vacío.")
                continue
            
            if "standings" in file_name:

                try:
                    # 4. Asumimos que hay un solo objeto JSON por archivo
                    raw_data_str = rows[0][0]  # primer dict completo del archivo
                    raw_data = json.loads(raw_data_str)

                    # 5. Extraer standings > table
                    table = raw_data["standings"][0]["table"]

                    # 6. Normalizar a DataFrame
                    df = pd.json_normalize(table)

                    # 7. Seleccionar columnas clave
                    df_final = df[[
                        "position", "team.name", "playedGames", "points",
                        "won", "draw", "lost", "goalsFor", "goalsAgainst", "goalDifference"
                    ]]

                    # 8. Guardar CSV
                    output_name = file_path.split('/')[-1].replace('.json', '_table.csv')
                    output_file = f"{folder_path}" + output_name
                    df_final.to_csv(output_file, index=False)

                    print(f"Guardado como: {output_file}")

                except Exception as e:
                    print(f"Error procesando {file_path}: {e}")
        