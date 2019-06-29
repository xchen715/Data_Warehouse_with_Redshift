import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into Rdshift staging Tables.
    
    Parameters
    ----------
        cur : psycopg2.connect.cursor
            cursor from psycopg2 connecting to Postgres database.
        
        conn : psycopg2.connect
            psycopg2 connection to Postgres database.
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into actual tables.
    
    Parameters
    ----------
        cur : psycopg2.connect.cursor
            cursor from psycopg2 connecting to Postgres database.
        
        conn : psycopg2.connect
            psycopg2 connection to Postgres database.
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('/home/wilson/Data_Engineering_NanoDegree/AWS_Keys/dwh.cfg')
    HOST = config.get('CLUSTER','HOST')
    DB_NAME = config.get('CLUSTER','DB_NAME')
    DB_USER = config.get('CLUSTER','DB_USER')
    DB_PASSWORD = config.get('CLUSTER','DB_PASSWORD')
    DB_PORT = config.get('CLUSTER','DB_PORT')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    conn.set_client_encoding('UTF8')
    #conn.set_client_encoding('UNICODE')
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()