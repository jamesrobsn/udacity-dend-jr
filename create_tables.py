import configparser
import psycopg2
# from sql_queries import create_table_queries, drop_table_queries
from sql.drop_queries import drop_table_queries
from sql.create_queries import create_table_queries


def drop_tables(cur, conn):
    '''
    Drops each table using the queries in `drop_table_queries` list.
    '''
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates each table using the queries in `create_table_queries` list. 
    '''
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connects to an AWS redshift cluster using connection details specified in dwh.cfg. Then drops tables if they exist and recreates them.
    '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()