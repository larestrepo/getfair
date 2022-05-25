#!/usr/bin/python
from utils import kobo_api
from dblib import create_tables, insert_project, read_query, write_query
import json


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
                    query = f"UPDATE {tableName} SET blockchain = 'TRUE' WHERE _id = '{_id}';"
                    write_query(query)
            if _id not in _id_array:
                # Setting up generic variables for the registry
                data_dict['project_id'] = project_ids[0]
                data_dict['_id'] = value['_id']
                data_dict['_uuid'] = value['_uuid']
                data_dict['usuario'] = value['group_usuario/nombre_usuario']
                data_dict['role'] = value['group_usuario/cargo_usuario']
                data_dict['dlocation'] = value['group_usuario/departamento_ubicacion']
                data_dict['mlocation'] = value['group_usuario/municipio_ubicacion']
                data_dict['gpslocation'] = value['group_usuario/ubicacion_usuario']
                data_dict['processed'] = "False"
                data_dict['blockchain'] = "False"
                data_dict['submission'] = value['_submission_time']
                validation_status = value['_validation_status']
                if validation_status != {}:
                    data_dict['blockchain'] = "True"
                    data_dict['validation'] = validation_status['label']
                tableName = 'data'
                columns = []
                for k in data_dict.keys():
                    columns = list(data_dict.keys())
                values = []
                for value in data_dict.values():
                    if type(value) == str:
                        value = value.replace("'", "''")
                        value = "'" + value + "'"
                    values += [str(value)]

                query = f"INSERT INTO {tableName}"
                query += "(" + ", ".join(columns) + ")\nVALUES"
                query += "(" + ", ".join(values) + "), \n"
                query = query[:-3] + " RETURNING id;"
                write_query(query)

    except Exception:
        print(f"Problems creating the data:  {_id}")


def create_measurements(data, meas_result):

    try:
        for value in data:
            _id = value['_id']
            exclusions = [
                "_id",
                'formhub/uuid',
                "group_usuario/nombre_usuario",
                "group_usuario/cargo_usuario",
                "group_usuario/nombre_instalacion",
                "group_usuario/departamento_ubicacion",
                "group_usuario/municipio_ubicacion",
                "group_usuario/ubicacion_usuario",
                "group_usuario/ubicacion_usuario_ts",
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
                            file_dict[keys[1] + '_' + keys[2]] = [v, instance, picture_id, download_url]
                        else:
                            measurement_dict[keys[1] + '_' + keys[2]] = v
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
            for i, (measurement_name, file) in enumerate(zip(measurement_dict, file_dict)):
                v = measurement_dict[measurement_name]
                file_name = file_dict[file][0]
                instance = file_dict[file][1]
                picture_id = file_dict[file][2]
                kobo_url = file_dict[file][3]
                if _id not in meas_ids and measurement_name not in meas_meas:
                    query = f"""INSERT INTO {tableName} (project_id, _id, measurement, value, file_name, instance, picture_id, kobo_url)
                    VALUES ({project_ids[0]}, {_id}, '{measurement_name}', {v}, '{file_name}', '{instance}', '{picture_id}', '{kobo_url}') RETURNING id;
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
    except TypeError:
        print("No projects found in table projects")
