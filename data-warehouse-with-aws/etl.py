import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time

def load_staging_tables(cur, conn):
    """"
    Copy song data and log data into staging tables.
    Note: it takes about 15 mins to load data into these 2 tables
    """
    
    start = time.time()
    for query in copy_table_queries:
        print('Loading table:', query)
        cur.execute(query)
        conn.commit()
    end = time.time()
    print(f'Loading Time: {end-start}')
    print('--- Done Loading Tables ---')
    
    
def insert_tables(cur, conn):
    """
    Inserting data from staging tables into fact and dimension tables.
    """
    for query in insert_table_queries:
        print('Inserting table:', query)
        cur.execute(query)
        conn.commit()
    print('--- Done Inserting Tables ---')

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) # connect to the cluster
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('--- Done ETL ---')
    conn.close()
    

if __name__ == "__main__":
    main()