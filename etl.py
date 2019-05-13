import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
"""
Description: This function can be used to load data to stage tables
Arguments:
    cur: the cursor object. 
    conn: connect to  database 

Returns:
    None
"""

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

 
def insert_tables(cur, conn):
"""
Description: This function can be used to insert data to fact and daimntion
Arguments:
    cur: the cursor object. 
    conn: connect to  database 

Returns:
    None
"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to  database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
     
    load_staging_tables(cur, conn)
   # insert_tables(cur, conn)
    # close connection to default database
    conn.close()


if __name__ == "__main__":
    main()