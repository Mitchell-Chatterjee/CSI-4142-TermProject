#!/usr/bin/python
import psycopg2
from config import config
import os
 
def testConnection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
   # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

# This method handles connecting to the database
def connect():
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        # return a cursor and the connection to the database
        return (conn, cur)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This method handles disconnecting from the database
# Must be called if the connect method is called upon ending the program
def disconnect(cur, conn):
    # terminate the cursor
    if cur is not None:
         cur.close()
    # terminate the database connection itself
    if conn is not None:
        conn.close()
        print('Database connection closed')

 
if __name__ == '__main__':
    # this the main test method to quickly check if your connection is working
    testConnection()

    # this is how to actually connect / disconnect fromt the database
    # print("acutal test")
    # connection, cursor = connect()
    # disconnect(connection, cursor)
    # os.system("PAUSE")

    