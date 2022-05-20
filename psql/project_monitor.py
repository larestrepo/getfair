#!/usr/bin/python
from utils import kobo_api
from dblib import create_tables, insert_project, read_query, build_column_values, write_query
import json
from datetime import date
import time

if __name__ == '__main__':

    BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets/"
    params = {
        'format': 'json'
    }
    rawResult = kobo_api(BASE_URL, params)
    rawResult = json.loads(rawResult.content.decode('utf-8'))

    # with open('./projects.json', 'w') as file:
    #     json.dump(rawResult, file, indent=4, ensure_ascii=False)
    
    create_tables()
    try:
        tableName = 'projects'
        query = f"SELECT uid FROM {tableName};"
        uids = read_query(query)
        uid_array = []
        if uids != [] or uids is not None:
            for uid in uids:
                uid_array.append(uid[0])

        projects = rawResult['results']
        for project in projects:
            deployment__active = project['deployment__active']
            owner__username = project['owner__username']
            uuid = project['uid']
            if deployment__active and uuid not in uid_array and owner__username=='getfair':
                # print(project['url'])
                # project = rawResult['results'][0]
                project_dict = {}
                try:
                    project_dict['name'] = project['name']
                    project_dict['country'] = project['settings']['country'][0]['label']
                    project_dict['sector'] = project['settings']['sector']['label']
                    # project_dict['description'] = project['settings']['description']
                    project_dict['url'] = project['url']
                    project_dict['owner'] = project['owner']
                    project_dict['uid'] = project['uid']
                    project_dict['kind'] = project['kind']
                    project_dict['asset_type'] = project['asset_type']
                    project_dict['version_id'] = project['version_id']
                    project_dict['date_created'] = project['date_created']

                    table = 'projects'
                    columns = []
                    for k in project_dict.keys():
                        columns = list(project_dict.keys())

                    values = []
                    for value in project_dict.values():
                        if type(value) ==str:
                            value = value.replace("'", "''")
                            value = "'" +value + "'"
                        values += [str(value)]
                    project_id = insert_project(table, columns, values)
                except Exception:
                    print(f"Error creating project {project['uid']}")
                    pass
        
        tableName = 'projects'
        query = f"SELECT id, uid FROM {tableName};"
        ids = read_query(query)
        for id in ids:
            ASSET_UID = id[1]
            URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'
            TODAY = date.fromtimestamp(time.time())
            QUERY = f'{{"_submission_time":{{"$gt":"{TODAY}"}}}}'
            params = {
                # 'query': QUERY,
                'format': 'json'
            }
            rawResult = kobo_api(URL, params)
            rawResult = json.loads(rawResult.content.decode('utf-8'))

            tableName = 'data'
            query = f"SELECT _id FROM {tableName};"
            _ids = read_query(query)
            _id_array = []
            if _ids != [] or _ids is not None:
                for _id in _ids:
                    _id_array.append(_id[0])
            for value in rawResult['results']:
                validation_status = value['_validation_status']
                _id = value['_id']
                if validation_status != {} and _id not in _id_array:
                    if validation_status['label'] == "Approved":
                        tableName = 'data'
                        columns = [
                            '_id',
                            '_uuid',
                        ]
                        query = f"INSERT INTO {tableName}"
                        column_list, values = build_column_values(columns, value)
                        print(id, column_list, values)

                        query += "(project_id, " + ", ".join(column_list) + ", processed)\nVALUES"
                        project_id_str = "'" + str(id[0]) + "', "
                        procssed_str = "'False'"
                        query += "(" + project_id_str + ", ".join(values) + "," + procssed_str + "), \n"
                        print(len(column_list), len(values))
                        query = query[:-3] + " RETURNING id;"
                        data_id = write_query(query)
    except TypeError:
        print(f"No projects found in table projects")