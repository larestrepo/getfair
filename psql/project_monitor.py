#!/usr/bin/python
from curses import raw
from utils import kobo_api
from dblib import create_tables, insert_project, read_query, build_column_values, write_query, insert_picture
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
        i = 1
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
                    project_dict['project_table'] = 'project' + str(i)
                    i += 1

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
        project_query_result = read_query(query)
        for project_ids in project_query_result:
            ASSET_UID = project_ids[1]
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
                        print(project_ids, column_list, values)

                        query += "(project_id, " + ", ".join(column_list) + ", processed)\nVALUES"
                        project_id_str = "'" + str(project_ids[0]) + "', "
                        procssed_str = "'False'"
                        query += "(" + project_id_str + ", ".join(values) + "," + procssed_str + "), \n"
                        print(len(column_list), len(values))
                        query = query[:-3] + " RETURNING id;"
                        data_id = write_query(query)

                        for k, v in value.items():
                            if '.png' in str(v):
                                # FileName = v.split(".")[0]
                                for attachment in value['_attachments']:
                                    if v == attachment['filename'].split('/')[-1]:
                                        instance = attachment['instance']
                                        id = attachment['id']
                                        download_url = f"{URL}{instance}/attachments/{id}/"
                                        # Insert picture data in table
                                        tableName = 'pictures'
                                        columns = [
                                            'project_id',
                                            'data_id',
                                            'id',
                                            'instance',
                                            'name',
                                            'url'
                                        ]
                                        values = (str(project_ids[0]), str(data_id), str(id), str(instance), str(v), str(download_url))
                                        picture_id = insert_picture(tableName, columns, values)
        
        # tableName = 'data'
        # query = f"SELECT id, project_id, _id, _uuid FROM {tableName} WHERE processed = 'FALSE';"
        # data_results = read_query(query)

        # for results in data_results:

        #     #Projects query
        #     tableName='projects'
        #     project_id = results[1]
        #     _id = results[2]
        #     query = f"SELECT id, name, country, sector, url, owner, uid, kind, asset_type, version_id, date_created, project_table FROM {tableName} WHERE id = '{project_id}';"
        #     project_info = read_query(query)

        #     # # ProjectX query
        #     # tableName=project_info[0][11]
        #     # query = f"SELECT * FROM {tableName} WHERE _id = '{_id}';"
        #     # record = read_query(query)

        #     # Data query
        #     tableName='data'
        #     query = f"SELECT id FROM {tableName} WHERE project_id='{project_id}'"
        #     data_id = read_query(query)
        #     data_id = data_id[0][0]
            
        #     # Download the attachments from the API
        #     ASSET_UID = project_info[0][6]
        #     URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'
        #     params = {
        #         'format': 'json'
        #     }
        #     rawResult = kobo_api(URL, params)
        #     rawResult = json.loads(rawResult.content.decode('utf-8'))

        #     for result in rawResult['results']:
        #         if _id == result['_id']:
                    
            
    except TypeError:
        print(f"No projects found in table projects")