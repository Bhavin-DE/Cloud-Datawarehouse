import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import pandas as pd
import boto3
import json


def drop_tables(cur, conn):
    """Used to drop tables
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Tables are dropped
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Used to create tables if it does not exist
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Tables are created
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Main function to run other python functions
    
    Args:
        N/A
    
    Output:
        1) Parses config file and derives variables
        2) Creates EC2 Instance on AWS
        3) Drops and Creates tables by executing python functions
    """
    #Parse config and derive variables
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY=config.get('AWS','KEY')
    SECRET= config.get('AWS','SECRET')
    
    #Create and EC2 Instance
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                                  )
    #Connect to DB
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except Exception as e:
        print(e)
                         
    #Drop Tables if it exists    
    print("Dropping Tables")
    drop_tables(cur, conn)
                         
    #Create Tables                     
    print ("Creating Tables")
    create_tables(cur, conn)

    #Close DB Connection
    conn.close()


if __name__ == "__main__":
    main()