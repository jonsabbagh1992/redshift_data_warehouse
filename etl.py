import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3
from aws_manager import AWSManager

def load_staging_tables(cur, conn):
    '''
    Loads raw staging tables collected from S3 storage. 
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Loads the final dimensional tables
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def run_etl():
    '''
    - Connects to the Redshift database
    - Loads staging tables to be used to further processing
    - Processes the staging tables  and loads them into the final star schema
    '''
    config = configparser.ConfigParser()
    CONFIG_FILE = 'dwh.cfg'
    config.read(CONFIG_FILE)
    

    DWH_DB                 = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DB_PORT")

    aws_manager = AWSManager()
    HOST = aws_manager.get_cluster_endpoint()
    
    conn = psycopg2.connect(f"host={HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()
    
    print("Loading Staging tables. Please wait...")
    load_staging_tables(cur, conn)
    print("Finished!\n")
    
    print("Loading dimensional tables. Please wait...")
    insert_tables(cur, conn)
    print("Finished!\n")
    
    conn.close()


if __name__ == "__main__":
    run_etl()