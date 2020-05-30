import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Used to load staging tables from raw files on S3
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Tables - staging_events & staging_songs
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Used to load dimension & fact tables from staging tables
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Dimension tables - artists, songs, time, users
        Fact table - songplays
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to run other python functions
    
    Args:
        N/A
    
    Output:
        1) Parses config file and derives variables
        2) create db connection
        2) load staging, dimension and fact tables
        3) close db connection
    """
    
    #Parse config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Create db connection
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)
        
    #Load Staging Tables
    print("loading staging tables")
    load_staging_tables(cur, conn)
    
    #Load Dimensions and Fact Tables
    print("loading dw tables")
    insert_tables(cur, conn)

    #Close connection
    conn.close()


if __name__ == "__main__":
    main()