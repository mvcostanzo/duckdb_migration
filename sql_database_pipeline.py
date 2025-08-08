import dlt
import json
import duckdb
import os
from sqlalchemy import create_engine, inspect
from dlt.sources.sql_database import sql_database
from dlt.sources.credentials import ConnectionStringCredentials

def get_configuration() -> dict:
    config_file = open(r'./config.json')
    config = json.load(config_file)
    return config

def build_connection_string(config: dict) -> str:
    connection_string = rf'{config['server']['drivername']}://{config['server']['host']}:{config['server']['port']}'
    return connection_string

def read_schemas(connection_string: str) -> list:
    engine = create_engine(connection_string)
    schema_inspector = inspect(engine)
    schema_list = schema_inspector.get_schema_names()
    return_list = []
    for schema in schema_list:
        table_list = schema_inspector.get_table_names(schema=schema)
        if table_list:
            return_list.append(schema)
        table_list = None
    return return_list


def truncate_and_shrink():
    config = get_configuration()
    for database in config['databases']:
        conn = duckdb.connect(rf'./{database}.duckdb')
        staging_schemas = conn.sql("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '%_staging'").fetchall()
        for schema_name in staging_schemas:
            conn.execute(rf'DROP SCHEMA {schema_name[0]} CASCADE')
        conn.close()

        duckdb.sql(rf"ATTACH './{database}.duckdb' as src")
        duckdb.sql(rf"ATTACH './{database}_compressed.duckdb' as dst")
        duckdb.sql("COPY FROM DATABASE src TO dst")
        duckdb.sql("DETACH src")
        duckdb.sql("DETACH dst")

        os.remove(rf'./{database}.duckdb')
        os.rename(rf'./{database}_compressed.duckdb', rf'./{database}.duckdb' )



def load_entire_database() -> None:
    
    config = get_configuration()
    connection_string_base = build_connection_string(config=config)

    for database in config['databases']:

        print (rf'Starting Database: {database}')

        full_connection_string = rf'{connection_string_base}/{database}'
        schemas = read_schemas(full_connection_string)

        credentials = ConnectionStringCredentials(full_connection_string)

        for schema in schemas:
            print (rf'Starting Ingestion: {database}.{schema}')
            # Define the pipeline
            pipeline = dlt.pipeline(
                pipeline_name= str.lower(database),
                destination='duckdb',
                dataset_name= str.lower(schema),
                pipelines_dir=r'./.dlt_pipelines'
            )

            # Fetch all the tables from the database
            source = sql_database(credentials=credentials, schema=schema).parallelize()
            
            # Run the pipeline
            info = pipeline.run(source, write_disposition="replace")

            # Print load info
            print(info)

if __name__ == '__main__':
    load_entire_database()
    truncate_and_shrink()