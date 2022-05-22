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
        i = 1
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
            validation_status = value['_validation_status']
            _id = value['_id']
            if validation_status != {} and _id not in _id_array and validation_status['label'] == "Approved":
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

                picture_id_array = create_picture(data_id, value)
                data_dict = {
                    data_id: picture_id_array
                }
    
        return data_dict
            
    except Exception:
        print(f"Problems creating the data:  {_id}")

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
        if not transaction_row:

            tableName = 'projects'
            query = f"SELECT id, name, country, sector, url, owner, uid, kind, asset_type, version_id, date_created FROM {tableName} WHERE id= '{project_id}'"
            project_result = read_query(query)
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
        "address_origin": [
            master_address,
            ],
            "address_destin":None,
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
            data = data['results']
            tableName = 'data'
            query = f"SELECT _id FROM {tableName};"
            _ids = read_query(query)
            _id_array = []
            if _ids != [] or _ids is not None:
                for _id in _ids:
                    _id_array.append(_id[0])
            data_dict = create_data(data, _id_array)
        # Check if transaction results are pending to be sent to the blockchain
        tableName = 'data'
        query = f"SELECT project_id, _id, id FROM {tableName} WHERE processed = 'FALSE';"
        data_results = read_query(query)
        if data_results != [] or data_results is not None:
            for data_result in data_results:

                # Build metadata 
                index = build_metadata(data_result)

                # Time to build the transaction draft
                tableName = 'transactions'
                query = f"SELECT metadata FROM {tableName} WHERE index = '{index}';"
                metadata = read_query(query)
                master_address = 'addr_test1qpr9xmkn4fpexdcd9e8kt8fektvqyrg3vetmw8pmmlava2kcyxgcpfar5pu5dlxx9y9c0mm2mtj48uz56q9aakvn2vksw633r5'
                metadata_array = metadata[0][0]
                for metadata in metadata_array:
                    params = transaction_build(master_address, metadata)
                    print(metadata)
                    node = base.Node()
                    estimated_fees = node.build_tx_components(**params)
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


        else:
            print("No data to process in the blockchain")
    except TypeError:
        print(f"No projects found in table projects")