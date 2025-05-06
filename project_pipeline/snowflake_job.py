import snowflake.connector
from dotenv import load_dotenv
import os
from pathlib import Path
import subprocess
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
        
    def upload_to_snowflake_datalake(self, local_folder_path:str, stage_name='@DATALAKE_FUTBOL', connection='my_connection') -> None:
        
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
              
sc = snowflake_connector()
sc.upload_to_snowflake_datalake("../data/temp_raw")
# sc.erase_everything_from_datalake()
