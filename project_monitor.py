#!/usr/bin/python
from utils import kobo_api, ipfs
from dblib import create_tables, insert_project, read_query, write_query, insert_picture
import json
from psycopg2.extras import Json


def create_projects(projects, uid_array):
    uid = None
    project_id_array = []
    try:
        if projects != []:
            for project in projects:
                deployment__active = project['deployment__active']
                owner__username = project['owner__username']
                uid = project['uid']
                if deployment__active and uid not in uid_array and owner__username == 'getfair':
                    project_dict = {}
                    project_dict['name'] = project['name']
                    project_dict['country'] = project['settings']['country'][0]['label']
                    project_dict['sector'] = project['settings']['sector']['label']
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
                        if type(value) == str:
                            value = value.replace("'", "''")
                            value = "'" + value + "'"
                        values += [str(value)]
                    project_id = insert_project(table, columns, values)
                    print(f"Project id {uid} was created")
                    project_id_array.append(project_id)
            return project_id_array
    except Exception:
        print(f"Problems creating project with id:  {uid}")


def create_data(data, _id_array):
    _id = None
    try:
        for value in data:
            data_dict = {}
            _id = value['_id']
            validation_status = value['_validation_status']
            if _id in _id_array:
                if validation_status != {}:
                    tableName = 'data'
                    query = f"UPDATE {tableName} SET validation = '{validation_status['label']}' WHERE _id = {_id} RETURNING id;"
                    write_query(query)
            if _id not in _id_array:
                # Setting up generic variables for the registry
                data_dict['project_id'] = project_ids[0]
                data_dict['_id'] = value.get('_id', None)
                data_dict['_uuid'] = value.get('_uuid', None)
                data_dict['usuario'] = value.get('group_presentacion/nombre_usuario', None)
                data_dict['role'] = value.get('group_presentacion/cargo_usuario', None)
                data_dict['dlocation'] = value.get('group_presentacion/departamento_ubicacion', None)
                data_dict['mlocation'] = value.get('group_presentacion/municipio_ubicacion', None)
                data_dict['gpslocation'] = value.get('group_presentacion/ubicacion_usuario', None)
                data_dict['submission'] = value.get('_submission_time', None)
                validation_status = value.get('_validation_status', None)
                if validation_status != {} or not None:
                    data_dict['validation'] = validation_status.get('label')
                all_in_dict = {}
                for k, v in value.items():
                    if k.startswith('group_presentacion'):
                        all_in_dict[k] = v
                data_dict['json'] = all_in_dict
                tableName = 'data'
                columns = list(data_dict.keys())
                values = []
                for value in data_dict.values():
                    if type(value) == str:
                        value = value.replace("'", "''")
                        value = "'" + value + "'"
                    if type(value) == dict:
                        value = Json(value)
                    if value == None:
                        value = 'NULL'
                    values += [str(value)]

                query = f"INSERT INTO {tableName}"
                query += "(" + ", ".join(columns) + ")\nVALUES"
                query += "(" + ", ".join(values) + "), \n"
                query = query[:-3] + " RETURNING id;"
                write_query(query)

    except Exception:
        print(f"Problems creating the data:  {_id} of project {project_ids[0]}")

def create_picture(data_result):
    try:
        
        project_id = data_result[0]
        _id = data_result[1]
        data_id = data_result[2]
        # Check if existing pictures are present
        tableName = 'pictures'
        query = f"SELECT picture_id FROM {tableName} WHERE project_id = '{project_id}' and data_id = '{data_id}'"
        existing_pictures = read_query(query)
        existing_pictures = list(zip(*existing_pictures))
        if existing_pictures == []:
            existing_pictures = [()]
        tableName = 'measurement'
        query = f"SELECT id, file_name, instance, picture_id, kobo_url FROM {tableName} WHERE project_id = '{project_id}' and _id = '{_id}'"
        picture_results = read_query(query)
        for picture_result in picture_results:
            measurement_id = picture_result[0]
            file_name = picture_result[1]
            instance = picture_result[2]
            picture_id = picture_result[3]
            kobo_url = picture_result[4]
            if picture_id not in existing_pictures[0] and picture_id != None:
                rawResultImage = kobo_api(kobo_url)
                # Store pictures in folder
                rawResultImage.raw.decode_content = True
                file_path = './pictures/' + file_name
                with open(file_path, 'wb') as file:
                    for chunk in rawResultImage.iter_content(chunk_size=16 * 1024):
                        file.write(chunk)
                    print('Image sucessfully Downloaded: ', file_name)
                # Upload picture in IPFS
                ipfs_result = ipfs(file_path)
                IPFS_HASH = ipfs_result['IpfsHash']
                # Update table with IPFS Hash
                # Insert picture data in table
                tableName = 'pictures'
                columns = [
                    'project_id',
                    'data_id',
                    'picture_id',
                    'instance',
                    'name',
                    'url',
                    'ipfshash',
                ]
                values = (str(project_id), str(data_id), str(picture_id), str(instance), str(file_name), str(kobo_url), str(IPFS_HASH))
                insert_picture(tableName, columns, values)
            else:
                print(f"No pictures to upload in project id: {project_id}, in _id: {_id} in data id: {data_id} and in measurement_id: {measurement_id}")
    except Exception:
        print(f"Problems creating picture with id:  {uid}")

def create_measurements(data, meas_result):

    try:
        for value in data:
            _id = value['_id']
            exclusions = [
                "_id",
                'formhub/uuid',
                "group_presentacion/nombre_usuario",
                "group_presentacion/nombre_usuario_ts",
                "group_presentacion/nombre_instalacion",
                '__version__',
                'meta/instanceID',
                '_xform_id_string',
                "_uuid",
                '_attachments',
                '_status',
                '_geolocation',
                "_submission_time",
                '_tags',
                '_notes',
                "_validation_status",
                "_submitted_by"
            ]
            download_url = None
            instance = None
            picture_id = None
            measurement_dict = {}
            file_dict = {}
            for keys, v in value.items():
                if keys not in exclusions:
                    if v != [] or v != {}:
                        keys = keys.split('/')[-1].split('_')
                        if 'Arc' in keys:
                            for attachment in value['_attachments']:
                                if v == attachment['filename'].split('/')[-1]:
                                    instance = attachment['instance']
                                    picture_id = attachment['id']
                                    download_url = f"{URL}{instance}/attachments/{picture_id}/"
                            file_dict[keys[0] + '_' + keys[1]] = [v, instance, picture_id, download_url]
                        else:
                            measurement_dict[keys[0] + '_' + keys[1]] = v
            # Create the first records in data table
            tableName = 'measurement'
            meas_ids = []
            meas_meas = []
            meas_values = []
            meas_files = []
            for meas in meas_result:
                meas_ids.append(meas[0])
                meas_meas.append(meas[1])
                meas_values.append(meas[2])
                meas_files.append(meas[3])
            
            if file_dict == {}:
                # Means that there are no attachment
                for k, v in measurement_dict.items():
                    if _id not in meas_ids:
                        query = f"""INSERT INTO {tableName} (project_id, _id, measurement, value, file_name, instance, picture_id, kobo_url)
                        VALUES ({project_ids[0]}, {_id}, '{k}', '{v}', NULL, NULL, NULL, NULL) RETURNING id;
                        """
                        write_query(query)
            else:
                for i, (measurement_name, file) in enumerate(zip(measurement_dict, file_dict)):
                    v = measurement_dict[measurement_name]
                    file_name = file_dict[file][0]
                    instance = file_dict[file][1]
                    picture_id = file_dict[file][2]
                    kobo_url = file_dict[file][3]
                    if _id not in meas_ids and measurement_name not in meas_meas:
                        query = f"""INSERT INTO {tableName} (project_id, _id, measurement, value, file_name, instance, picture_id, kobo_url)
                        VALUES ({project_ids[0]}, {_id}, '{measurement_name}', '{v}', '{file_name}', '{instance}', '{picture_id}', '{kobo_url}') RETURNING id;
                        """
                        write_query(query)

    except Exception:
        print("Problems creating the data")


if __name__ == '__main__':

    create_tables()

    BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets/"
    params = {
        'format': 'json'
    }
    rawResult = kobo_api(BASE_URL, params)
    rawResult = json.loads(rawResult.content.decode('utf-8'))
    with open('./rawresult.json', 'w') as file:
        json.dump(rawResult, file, indent=4, ensure_ascii=False)
    if 'results' in rawResult:
        projects = rawResult['results']
    else:
        projects = []
    try:
        tableName = 'projects'
        query = f"SELECT uid FROM {tableName};"
        uids = read_query(query)
        uid_array = []
        if uids != [] or uids is not None:
            for uid in uids:
                uid_array.append(uid[0])

        project_id = create_projects(projects, uid_array)

        tableName = 'projects'
        query = f"SELECT id, uid FROM {tableName};"
        project_query_result = read_query(query)
        for project_ids in project_query_result:
            project_id = project_ids[0]
            ASSET_UID = project_ids[1]
            URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'
            params = {
                'format': 'json'
            }
            rawResult = kobo_api(URL, params)
            data = json.loads(rawResult.content.decode('utf-8'))
            with open('./data.json', 'w') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            if 'results' in data:
                data = data['results']
            else:
                continue
            tableName = 'data'
            query = f"SELECT _id FROM {tableName};"
            _ids = read_query(query)
            _id_array = []
            if _ids != [] or _ids is not None:
                for _id in _ids:
                    _id_array.append(_id[0])
            # Insert new data or update data if validation is true
            create_data(data, _id_array)
            tableName = 'measurement'
            query = f"SELECT _id, measurement, value, file_name FROM {tableName};"
            meas_result = read_query(query)
            create_measurements(data, meas_result)
            # Check if transaction results are pending to be sent to the blockchain
            tableName = 'data'
            query = f"SELECT project_id, _id, id FROM {tableName} WHERE validation = 'Approved' and project_id='{project_id}';"
            data_results = read_query(query)
            if data_results != [] or data_results is not None:
                for data_result in data_results:
                    create_picture(data_result)
    except TypeError:
        print("No projects found in table projects")
