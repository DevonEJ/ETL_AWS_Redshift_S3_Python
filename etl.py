import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Iterates over a list of SQL queries for loading staging tables from Amazon S3 buckets, executing these 
    using the database cursor and connection objects passed in.
    
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Iterates over a list of SQL queries for loading analytics tables in Redshift from staging tables, executing these 
    using the database cursor and connection objects passed in.
    
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Function reads a configuration file, and uses this to create a connection to a specified database in Amazon
    Redshift, using credentials provided in the config file.
    
    Function then calls two nested functions for executing SQL queries against the database connection, before
    closing the connection.
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
