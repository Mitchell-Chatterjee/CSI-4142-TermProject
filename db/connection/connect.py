#!/usr/bin/python
import psycopg2
from config import config
import os

class Database:
    def __init__(self):
        self.connection = None
        try:
            # read connection parameters
            params = config()
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.connection = psycopg2.connect(**params)
            # create a cursor
            self.cursor = self.connection.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def __del__(self):
        self.disconnect()
        
    
    def testConnection(self):
        """ Connect to the PostgreSQL database server """
        try:
            # execute a statement
            print('PostgreSQL database version:')
            self.cursor.execute('SELECT version()')
    
            # display the PostgreSQL database server version
            db_version = self.cursor.fetchone()
            print(db_version)
        
            # close the communication with the PostgreSQL
            self.cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    # This method handles disconnecting from the database
    # Must be called if the connect method is called upon ending the program
    def disconnect(self):
        # terminate the cursor
        if self.cursor is not None:
            self.cursor.close()
        # terminate the database connection itself
        if self.connection is not None:
            self.connection.close()
            print('Database connection closed')

    
if __name__ == '__main__':
    # this the main test method to quickly check if your connection is working
    # to use this class just remove the conn.testConnection() method
    conn = Database()
    conn.testConnection()
    del conn

    os.system("PAUSE")
    