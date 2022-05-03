

"""
Steps are:

1. Get the image from the API
2. Store the image in local directory
3. Call the IPFS procedure

"""

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
