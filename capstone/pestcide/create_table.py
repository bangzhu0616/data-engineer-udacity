import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the pestcidedb
    - Returns the connection and cursor to pestcidedb
    """

    conn = psycopg2.connect('host=127.0.0.1 dbname=bangzhu user=bangzhu')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS pestcidedb")
    cur.execute("CREATE DATABASE pestcidedb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=pestcidedb user=bangzhu")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the pestcidedb database. 
    
    - Establishes connection with the pestcidedb database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()