from project_pipeline.api_call import extract_api_data
from project_pipeline.snowflake_job import snowflake_connector


if __name__ == "__main__":
    print("Iniciando extracción")
    extract_api_data()
    print("Iniciando conexión")
    sc = snowflake_connector()
    print("Subiendo datos")
    sc.upload_to_snowflake_datalake("data/temp_raw")