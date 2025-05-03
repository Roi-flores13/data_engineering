import snowflake.connector
from dotenv import load_dotenv
import os
from pathlib import Path
import subprocess
load_dotenv()

class snowflake_connector:
    def __init__(self) -> None:
        PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        USER = os.getenv("SNOWFLAKE_USER")
        ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        SCHEMA = os.getenv("SCHEMA")
        DATABASE = os.getenv("DATABASE")
        
        self.conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        database=DATABASE,
        schema=SCHEMA
        )
        
        print("conexión establecida con Snowflake")
        
    def upload_to_snowflake_datalake(self, local_file_path, stage_name='@DATALAKE_FUTBOL', connection='my_connection'):
        file_path = Path(local_file_path).resolve()
        file_path = file_path.as_posix()

        command = [
            'snowsql',
            '-c', connection,
            '-q', f"PUT file://{file_path} {stage_name} AUTO_COMPRESS=FALSE"
        ]

        try:
            print(f"Ejecutando: {' '.join(command)}")
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print("Subido correctamente:")
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print("Error al subir archivo:")
            print(e.stderr)
        
sc = snowflake_connector()
sc.upload_to_snowflake_datalake("../data/raw/PL_json.json")





