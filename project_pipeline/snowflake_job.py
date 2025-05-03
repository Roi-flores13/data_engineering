import snowflake.connector
from dotenv import load_dotenv
import os
from pathlib import Path
import subprocess
load_dotenv()

class snowflake_connector:
    def __init__(self) -> None:
        
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
        
    def upload_to_snowflake_datalake(self, local_file_path:str, stage_name='@DATALAKE_FUTBOL', connection='my_connection') -> None:
        
        file_path = Path(local_file_path).resolve() # Extrae el path del archivo a subir
        file_path = file_path.as_posix() # Cambia las diagonales para que no haya errores

        # Armar el comando que se escribiría en la terminal para hacer el PUT en Snowflake 
        command = [
            'snowsql',
            '-c', connection,
            '-q', f"PUT file://{file_path} {stage_name} AUTO_COMPRESS=FALSE"
        ]

        # Se intenta subir el archivo al DataLake de SnowFlake
        try:
            print(f"Ejecutando: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, check=True) # Se corre el comando como si fuera en la terminal
            print("Subido correctamente:")
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print("Error al subir archivo:")
            print(e.stderr)
        
sc = snowflake_connector()
sc.upload_to_snowflake_datalake("../data/raw/PL_json.json")