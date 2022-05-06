#!/usr/bin/python
from configparser import ConfigParser
import psycopg2

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect():
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

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
            CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            name VARCHAR (255) NOT NULL,
            country VARCHAR (255),
            sector VARCHAR (255),
            description text,
            url text,
            owner VARCHAR(255),
            uid text,
            kind VARCHAR(255),
            asset_type VARCHAR(255),
            version_id text,
            date_created TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS data (
            id SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            start_date TIMESTAMP, 
            end_date TIMESTAMP,
            subscriberid bigint,
            deviceid text,
            Foto_Arbol text,
            PyeHassGps text,
            Evaluacion_de_aplicacion text,
            Tipo_de_plaga_Aplicacion text,
            Planta_afectada text,
            tipo_de_plaga text,
            Finca text,
            _id BIGINT,
            _uuid TEXT,
            _validation_status text,
            processed BOOLEAN,
            FOREIGN KEY (project_id) 
                REFERENCES projects (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pictures (
            index SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL,
            data_id INTEGER NOT NULL,
            id bigint NOT NULL,
            instance bigint NOT NULL,
            name VARCHAR (255),
            url text,
            ipfshash text,
            FOREIGN KEY (project_id) 
                REFERENCES projects (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (data_id)
                REFERENCES data (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """,
                """
        CREATE TABLE IF NOT EXISTS transactions (
            index SERIAL PRIMARY KEY,
            data_id INTEGER NOT NULL,
            tx_hash text,
            time TIMESTAMP,
            address_origin text,
            address_destin text,
            metadata text,
            fees BIGINT,
            FOREIGN KEY (data_id)
                REFERENCES data (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        );
        """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_project(tableName, columns, values):
    """ insert multiple projects into the projects table """


    query = f"INSERT INTO {tableName} "
    query += f"(" + ", ".join(columns) + ")\nVALUES"
    query += "(" + ", ".join(values) + "), \n"
    query = query[:-3] + " RETURNING id;"

    conn = None
    project_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(query)
        # get the generated id back
        project_id = cur.fetchone()
        project_id = project_id[0] # type: ignore
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return project_id

def insert_picture(tableName, columns, values):
    """ insert multiple projects into the projects table """


    query = f"INSERT INTO {tableName}"
    query += "(" + ", ".join(columns) + ")\nVALUES(%s,%s,%s,%s,%s,%s) RETURNING index;"

    conn = None
    picture_id = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(query, values)
        # get the generated id back
        picture_id = cur.fetchone()[0] # type: ignore
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return picture_id


if __name__ == '__main__':
    connect()