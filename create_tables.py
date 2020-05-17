import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3
from aws_manager import AWSManager

def drop_tables(cur, conn):
    '''
    Drops all tables in the Redshift database so that the ETL script can be rerun.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates all Redshift tables.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def run_initial_setup():
    '''
    - Drops all tables in the Redshift database so that the ETL script can be rerun.
    - Creates all Redshift tables. 
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

    print("Resetting Tables.")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Finished!\n")
    
    conn.close()

if __name__ == "__main__":
    run_initial_setup()