import psycopg2
import requests
from requests.structures import CaseInsensitiveDict
import os
from decouple import config
from pinata import PinataPy


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
        id = None

        try:
            cur.execute( query )
            id = cur.fetchone()[0]
            conn.commit()

            print ('\nfinished CREATE OR INSERT TABLES execution')

        except (Exception, psycopg2.Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()

        # close the cursor and connection
        cur.close()
        conn.close()
        return id

def read_query(query):
    cur, conn = connect_database()
    if cur != None:

        try:
            cur.execute( query )
            conn.commit()

            print ('\nfinished SELECT execution')
            return cur.fetchall()

        except (Exception, psycopg2.Error) as error:
            print("\nexecute_sql() error:", error)
            conn.rollback()
            return None 

        # close the cursor and connection
        cur.close()
        conn.close()

def kobo_api(URL, params= {}):
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Token f820e2c138e487e28c87fbb9af4685f7f68051a4"

    resp = requests.get(URL, headers=headers, params=params)
    rawResult = resp
    return rawResult

def ipfs(file_path_name):
    #Inputs
    PINATA_API_KEY= config('PINATA_API_KEY')
    PINATA_SECRET_API_KEY = config('PINATA_SECRET_API_KEY')

    c = PinataPy(PINATA_API_KEY,PINATA_SECRET_API_KEY)
    cid = c.pin_file_to_ipfs(file_path_name)
    print(f"IPFS hash is: {cid}")
    return cid

"""
Activate when running manually
"""

if __name__ == '__main__':
    query = "SELECT * FROM projects;"
    result = read_query(query)
