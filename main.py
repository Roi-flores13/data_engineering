from project_pipeline.api_call import api_manager
from project_pipeline.snowflake_job import snowflake_connector


if __name__ == "__main__":
    AM = api_manager()
    SC = snowflake_connector()

    # AM.extract_api_data()
    # SC.upload_to_snowflake_datalake()
    
    SC.erase_everything_from_datalake()
    
    # SC.extraer_tabla_desde_json_stage()
    # api_manager.api_data_eraser("data/processed")