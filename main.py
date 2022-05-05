import json
from datetime import date
import time
from utils import read_query, write_query, kobo_api, ipfs
import psycopg2

import requests
from requests.structures import CaseInsensitiveDict

from psql import dblib

""" Curl equivalent
curl -X GET https://kf.kobotoolbox.org/api/v2/assets/auUF7gnnmorhZ7vSsMSozR/data.json -H 
"Authorization: Token f820e2c138e487e28c87fbb9af4685f7f68051a4"

Usefull page:
https://reqbin.com/req/python/c-xgafmluu/convert-curl-to-python-requests

https://realpython.com/python-api/

This script is doing the following:

1. Get latest results from Kobo by using API and create the json file with the results
2. Write data in postgres database in data table. (The project needs to be created in projects table)
3. Processing images

"""


if __name__ == '__main__':

    # TODO
    # 4. Handle response errors
    BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets/"
    ASSET_UID = "auUF7gnnmorhZ7vSsMSozR"
    URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'

    # TODAY = date.fromtimestamp(time.time())
    TODAY = '2022-05-04'

    QUERY = f'{{"_submission_time":{{"$gt":"{TODAY}"}}}}'
    params = {
        'query': QUERY,
        'format': 'json'
    }
    rawResult = kobo_api(URL, params)
    rawResult = json.loads(rawResult.content.decode('utf-8'))

    with open('./Aguacate_project_filtered.json', 'w') as file:
        json.dump(rawResult, file, indent=4, ensure_ascii=False)

    # Get the Project ID Index
    tableName = 'projects'
    query = f"SELECT id FROM {tableName} WHERE uid= '{ASSET_UID}'"
    rawResult_fromProjects = read_query(query)
    if rawResult_fromProjects == []:
        print(f'Results = {rawResult_fromProjects}')
    else:
        project_id = rawResult_fromProjects[0][0]

        # Build the insert query
        for register in rawResult['results']:
            column_list = []
            values = []
            tableName = 'data'
            # TODO: read this list from json which was used to create the table as well. 
            columns = [
                'start',
                'end',
                'subscriberid',
                'deviceid',
                'Foto_Arbol',
                'PyeHassGps',
                'Evaluacion_de_aplicacion',
                'Tipo_de_plaga_Aplicacion',
                'Planta_afectada',
                'tipo_de_plaga',
                'Finca',
                '_id',
                '_uuid',
                '_validation_status'
            ]
            query = f"INSERT INTO {tableName}"
            for column in columns:
                # This is a way to take only the values needed to build the DB table
                if column in register:
                    if register[column]:
                        column_list.append(column)
                        # If it is a string, the value should go with ''
                        if type(register[column]) ==str:
                            value = register[column].replace("'", "''")
                            value = "'" +value + "'"
                        else:
                            value = register[column]
                        values += [str(value)]

            # The source data comes with start and end but these are reserved words in postgres, so need to change to start_date and end_date
            column_list[0]= 'start_date'
            column_list[1]= 'end_date'
            
            query += "(project_id, " + ", ".join(column_list) + ")\nVALUES"
            project_id_str = "'" + str(project_id) + "', "
            query += "(" + project_id_str + ", ".join(values) + "), \n"
            print(len(column_list), len(values))
            query = query[:-3] + " RETURNING id;"
            data_id = write_query(query)
            column_list = []
            values = []

            # Find the main image associated to the registry
            MainImageName = register['Foto_Arbol']
            for attachment in register['_attachments']:
                if MainImageName == attachment['filename'].split("/")[-1]:
                    instance = attachment['instance']
                    id = attachment['id']
            
            # Insert picture data in table
            download_url = f"{URL}{instance}/attachments/{id}/"
            rawResultImage = kobo_api(download_url)
            tableName = 'pictures'
            columns = [
                'project_id',
                'data_id',
                'id',
                'instance',
                'name',
                'url',
                'picture_data'
            ]
            values = (str(project_id),
                str(data_id),
                str(id),
                str(instance),
                "'" + str(MainImageName) + "'",
                "'" + str(download_url) + "'",
                psycopg2.Binary(rawResultImage.content))
            
            picture_id = dblib.insert_picture(tableName, columns, values)
            # Store pictures in folder
            rawResultImage.raw.decode_content = True
            file_path = './pictures/' + MainImageName
            with open(file_path, 'wb') as file:
                for chunk in rawResultImage.iter_content(chunk_size=16*1024):
                    file.write(chunk)
                print('Image sucessfully Downloaded: ', MainImageName)
            
            # Upload picture in IPFS
            ipfs_result = ipfs(file_path)
            IPFS_HASH = ipfs_result['IpfsHash']
            
            # Update table with IPFS Hash
            query = f"UPDATE {tableName} SET ipfshash = '{IPFS_HASH}' WHERE index = {picture_id} RETURNING id"
            data_id = write_query(query)


