import json
import psycopg2
from psycopg2.extras import Json


def connect_database():
    try:
        conn=psycopg2.connect("dbname='postgres' user='getfair' password='getfairproject' host='getfair.c9pnmejp0cev.us-east-1.rds.amazonaws.com' port='5432' ") 
        cur=conn.cursor()
        # print ("\ncreated cursor object:", cur)
        return cur, conn
    except (Exception, psycopg2.Error) as err:
        print ("\npsycopg2 connect error:", err)
        conn = None
        cur = None
        return cur, conn

def write_query(query):
    cur, conn = connect_database()
    if cur != None:

        try:
            cur.execute( query )
            conn.commit()

            print ('\nfinished CREATE OR INSERT TABLES execution')

        except (Exception, psycopg2.Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()

        # close the cursor and connection
        cur.close()
        conn.close()

def read_query(query):
    cur, conn = connect_database()
    if cur != None:

        try:
            cur.execute( query )
            conn.commit()

            print ('\nfinished SELECT execution')
            print("The number of parts: ", cur.rowcount)
            return cur.fetchall()

        except (Exception, psycopg2.Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()
            return None 

        # close the cursor and connection
        cur.close()
        conn.close()

"""
Activate when running manually
"""

if __name__ == '__main__':
    query = "SELECT * FROM projects;"
    result = read_query(query)
