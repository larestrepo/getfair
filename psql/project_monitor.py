#!/usr/bin/python
from utils import kobo_api, ipfs
from dblib import create_tables, insert_project, read_query, build_column_values, write_query, insert_picture
import json
from psycopg2.extras import Json
from CardanoPython.src.cardano import base

def create_projects(projects, uid_array):
    uid = None
    project_id_array = []
    try:
        for project in projects:
            deployment__active = project['deployment__active']
            owner__username = project['owner__username']
            uid = project['uid']
            if deployment__active and uid not in uid_array and owner__username=='getfair':
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
                    if type(value) ==str:
                        value = value.replace("'", "''")
                        value = "'" +value + "'"
                    values += [str(value)]
                project_id = insert_project(table, columns, values)
                print(f"Project id {uid} was created")
                project_id_array.append(project_id)
        return project_id_array
    except Exception:
        print(f"Problems creating project with id:  {uid}")

def create_picture(data_id, value):
    try:
        picture_id_array = []
        for key, v in value.items():
            if '.png' in str(v):
                for attachment in value['_attachments']:
                    if v == attachment['filename'].split('/')[-1]:
                        instance = attachment['instance']
                        id = attachment['id']
                        #Download the picture
                        download_url = f"{URL}{instance}/attachments/{id}/"
                        rawResultImage = kobo_api(download_url)
                        # Store pictures in folder
                        rawResultImage.raw.decode_content = True
                        file_path = './pictures/' + v
                        with open(file_path, 'wb') as file:
                            for chunk in rawResultImage.iter_content(chunk_size=16 * 1024):
                                file.write(chunk)
                            print('Image sucessfully Downloaded: ', v)
                        # Upload picture in IPFS
                        ipfs_result = ipfs(file_path)
                        IPFS_HASH = ipfs_result['IpfsHash']
                        # Update table with IPFS Hash
                        # Insert picture data in table
                        tableName = 'pictures'
                        columns = [
                            'project_id',
                            'data_id',
                            'id',
                            'instance',
                            'name',
                            'url',
                            'ipfshash',
                        ]
                        values = (str(project_ids[0]), str(data_id), str(id), str(instance), str(v), str(download_url), str(IPFS_HASH))
                        picture_id = insert_picture(tableName, columns, values)
                        picture_id_array.append(picture_id)
        return picture_id_array
    except Exception:
        print(f"Problems creating picture with id:  {uid}")

def create_data(data, _id_array):
    data_dict = {}
    _id = None
    try:
        for value in data:
            _id = value['_id']
            validation_status = value['_validation_status']
            if _id in _id_array:
                if validation_status != {}:
                    tableName = 'data'
                    query = f"UPDATE {tableName} SET blockchain = 'TRUE' WHERE _id = '{_id}' returning index;"
                    data_id = write_query(query)
            if _id not in _id_array:
                # Setting up generic variables for the registry
                data_dict['project_id']= project_ids[0]
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
                    if type(value) ==str:
                        value = value.replace("'", "''")
                        value = "'" +value + "'"
                    values += [str(value)]

                query = f"INSERT INTO {tableName}"
                query += "(" + ", ".join(columns) + ")\nVALUES"
                query += "(" + ", ".join(values) + "), \n"
                query = query[:-3] + " RETURNING id;"
                print(query)
                data_id = write_query(query)
            
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
            pairs = []
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
                                    id = attachment['id']
                                    download_url = f"{URL}{instance}/attachments/{id}/"
                            file_dict[keys[1] + '_' + keys[2]] = [v, download_url]
                        else:
                            measurement_dict[keys[1] + '_' + keys[2]] = v
            pairs = list(zip(measurement_dict, file_dict))
            print(pairs)
            # pairs.append((measurement_dict, file_dict))
            # print(pairs)

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
                kobo_url = file_dict[file][1]
                if _id not in meas_ids and measurement_name not in meas_meas:
                    query = f"""INSERT INTO {tableName} (project_id, _id, measurement, value, file_name, kobo_url)
                    VALUES ({project_ids[0]}, {_id}, '{measurement_name}', {v}, '{file_name}', '{kobo_url}') RETURNING id;
                    """
                else:
                    query = f"""UPDATE {tableName} SET value = {v}, file_name = '{file_name}', kobo_url = '{kobo_url}' WHERE measurement = '{measurement_name}';
                    """
                measurement_id = write_query(query)

        return measurement_id
            
    except Exception:
        print(f"Problems creating the data in bcprojects:  {_id}")

def build_metadata(data_result):
    # Exclusion fields
    exclusions = [
        '_id',
        'formhub/uuid',
        'meta/instanceID',
        '_xform_id_string',
        '_uuid',
        '_attachments',
        '_status',
        '_submission_time',
        '_tags',
        '_notes',
        '_validation_status',
        '__version__',
        '_geolocation'
        ]
    try:
        main_metadata = []
        # Get the project UID
        project_id = data_result[0]
        _id = data_result[1]
        data_id = data_result[2]

        # verify if metadata is already in transactions table
        tableName = 'transactions'
        query = f"SELECT index FROM {tableName} WHERE data_id = '{data_id}';"
        transaction_row = read_query(query)
        tableName = 'projects'
        query = f"SELECT id, name, country, sector, url, owner, uid, kind, asset_type, version_id, date_created FROM {tableName} WHERE id= '{project_id}'"
        project_result = read_query(query)
        if not transaction_row and project_result is not None:

            # Build generic metadata
            generic_metadata = {}
            generic_metadata['name'] = project_result[0][1]
            generic_metadata['country'] = project_result[0][2]
            generic_metadata['sector'] = project_result[0][3]
            generic_metadata['kind'] = project_result[0][7]
            generic_metadata['asset_type'] = project_result[0][8]
            ASSET_UID = project_result[0][6]
            URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'
            params = {
                'format': 'json'
            }
            rawResult = kobo_api(URL, params)
            data = json.loads(rawResult.content.decode('utf-8'))
            data = data['results']
            data_metadata_array = []
            for value in data:
                # Build the metadata
                data_metadata = {}
                if _id == value['_id']:
                    for keys, v in value.items():
                        if keys not in exclusions:
                            if v != [] or v != {}:
                                if '.png' in str(v):
                                    tableName = 'pictures'
                                    query = f"SELECT ipfshash FROM {tableName} WHERE name= '{v}'"
                                    ipfshash_result = read_query(query)
                                    if ipfshash_result is None:
                                        print("Problem with some of the documents. No ipfs hash available")
                                        data_metadata = {}
                                        break
                                    ipfshash = ipfshash_result[0][0]
                                    v = 'ipfs://' + ipfshash

                                keys= keys.split('/')[-1]
                                data_metadata[keys] = v
                    data_metadata_array.append(data_metadata)

            # By project
                if len(data_metadata_array) > 63:
                    main_metadata.append({
                        "294857930485": {
                            "project_info": generic_metadata,
                            "data_info": data_metadata_array
                        }
                    })
                    data_metadata_array = []
            main_metadata.append({
                "294857930485": {
                    "project_info": generic_metadata,
                    "data_info": data_metadata_array
                }
            })
            tableName = 'transactions'
            query = f"INSERT INTO {tableName} (data_id, metadata)\nVALUES ({str(data_id)}, {Json(main_metadata)}) returning index;"
            transaction_id = write_query(query)
            print(f"Metadata updated succesfully with transaction ID: {transaction_id}")
        else:
            transaction_id = transaction_row[0][0]
            print(f"Records already exists in transaction table: {transaction_id}")
        return transaction_id
        
    except AssertionError:
        print("Problem with some of the documents. No ipfs hash available")

def transaction_build(master_address, metadata):

    witness = 1
    params = {
        "message": {
            "tx_info": {
                "address_origin": master_address,
                "address_destin": None,
                "change_address": master_address,
                "metadata": metadata,
                "mint": None,
                "script_path": None,
                "witness": witness,
            }
        }
    }
    return params


if __name__ == '__main__':

    create_tables()

    BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets/"
    params = {
        'format': 'json'
    }
    rawResult = kobo_api(BASE_URL, params)
    rawResult = json.loads(rawResult.content.decode('utf-8'))
    projects = rawResult['results']
    
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
            bcprojects_id = create_measurements(data, meas_result)
        # Check if transaction results are pending to be sent to the blockchain
        tableName = 'data'
        query = f"SELECT project_id, _id, id FROM {tableName} WHERE blockchain = 'TRUE' and processed = 'FALSE';"
        data_results = read_query(query)
        if data_results != [] or data_results is not None:
            for data_result in data_results:

                # Build metadata 
                index = build_metadata(data_result)

                # Time to build the transaction draft
                tableName = 'transactions'
                query = f"SELECT metadata FROM {tableName} WHERE index = '{index}';"
                metadata = read_query(query)
                master_address = 'MasterGetFair'
                metadata_array = metadata[0][0]
                for metadata in metadata_array:
                    params = transaction_build(master_address, metadata)
                    print(metadata)
                    node = base.Node()
                    estimated_fees = node.build_tx_components(**params)
                    fees = 0
                    if estimated_fees is not None:
                        fees = estimated_fees.split(' ')[-1]
                    # Analyze the transaction
                    tx_name_file = 'tx.draft'
                    tx_result = node.analyze_tx(tx_name_file)
                    if tx_result is not None:
                        tx_result = tx_result.splitlines()
                        tx_utxo_input = tx_result[6].split(' ')[-1]
                    else:
                        tx_utxo_input = None
                    
                    with open(node.TRANSACTION_PATH_FILE + '/' + tx_name_file, 'r') as file:
                        cbor_hex = json.load(file)
                    network = node.CARDANO_NETWORK
                    # Store the transaction draft in database
                    tableName = 'transactions'
                    query = f"UPDATE {tableName} SET address_origin = '{master_address}', txin = '{tx_utxo_input}', tx_cborhex = {Json(cbor_hex)}, fees='{fees}', network='{network}' returning index;"
                    transaction_id = write_query(query)
                    transaction_sign_result = node.sign_transaction(master_address)
                    transaction_submit_result = node.submit_transaction()
                    TxHash = node.get_txid()
                    # Store the transaction result in database
                    tableName = 'transactions'
                    query = f"UPDATE {tableName} SET tx_hash = '{TxHash}' where index='{transaction_id}' returning index;"
                    transaction_id = write_query(query)
                    tableName = 'data'
                    query = f"UPDATE {tableName} SET processed = 'True' WHERE id='{data_result[2]}' returning id;"
                    data_id = write_query(query)
                    print(f"transaction succesfully submitted with Hash: {TxHash} in table 'data' with id: {data_id} and 'table' transactions with index: {transaction_id}")
        else:
            print("No data to process in the blockchain")
    except TypeError:
        print(f"No projects found in table projects")