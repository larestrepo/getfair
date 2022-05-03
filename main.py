import requests
from requests.structures import CaseInsensitiveDict
import json
from datetime import date
import time
from psql_interact import read_query, write_query

""" Curl equivalent
curl -X GET https://kf.kobotoolbox.org/api/v2/assets/auUF7gnnmorhZ7vSsMSozR/data.json -H 
"Authorization: Token f820e2c138e487e28c87fbb9af4685f7f68051a4"

Usefull page:
https://reqbin.com/req/python/c-xgafmluu/convert-curl-to-python-requests

https://realpython.com/python-api/


"""

def kobo_api(url, params):
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Token f820e2c138e487e28c87fbb9af4685f7f68051a4"

    resp = requests.get(URL, headers=headers, params=params)
    rawResult = json.loads(resp.content.decode('utf-8'))
    return rawResult


if __name__ == '__main__':

    # TODO
    # 2. Upload results into DB
    # 3. See how to download image files
    # 4. Handle response errors
    BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets"
    ASSET_UID = "auUF7gnnmorhZ7vSsMSozR"
    URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'

    TODAY = date.fromtimestamp(time.time())
    # TODAY = '2022-05-02'

    QUERY = f'{{"_submission_time":{{"$gt":"{TODAY}"}}}}'
    params = {
        'query': QUERY,
        'format': 'json'
    }
    rawResult = kobo_api(URL, params)

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

        tableName = 'data'
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

        #Create filtered dictionary
        results_dict = {}
        values = []
        for register in rawResult['results']:
            column_list = []
            values = []
            query = f"INSERT INTO {tableName}"
            for column in columns:
                if column in register:
                    if register[column]:
                        column_list.append(column)
                        if type(register[column]) ==str:
                            value = register[column].replace("'", "''")
                            value = "'" +value + "'"
                        else:
                            value = register[column]
                        values += [str(value)]

            column_list[0]= 'start_date'
            column_list[1]= 'end_date'
            
            query += "(project_id, " + ", ".join(column_list) + ")\nVALUES"
            project_id_str = "'" + str(project_id) + "', "
            query += "(" + project_id_str + ", ".join(values) + "), \n"
            print(len(column_list), len(values))
            query = query[:-3] + ";"
            result = write_query(query)
