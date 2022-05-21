
from psql.dblib import read_query, build_column_values, write_query
from utils import kobo_api
import json

def insert_metadata(data_id, main_metadata):
    tableName = 'transactions'
    columns = [
        'data_id',
        'metadata',
    ]
    query = f"INSERT INTO {tableName}"
    for metadata in main_metadata:
        values = (str(data_id), str(metadata))
        column_list, values = build_column_values(columns, value)

        query += "(".join(column_list) + ")\nVALUES"
        query += "(".join(values) + "), \n"
    query = query[:-3] + " RETURNING id;"
    transaction_id = write_query(query)
    return transaction_id


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
    '__version__'
    ]
try:
    # query postgres to get the data
    tableName = 'data'
    query = f"SELECT project_id, _id, id FROM {tableName} WHERE processed = 'FALSE';"
    data_results = read_query(query)

    if data_results != [] or data_results is not None:
        main_metadata = []
        for data_result in data_results:
            # Get the project UID
            project_id = data_result[0]
            _id = data_result[1]
            data_id = data_result[2]
            tableName = 'projects'
            query = f"SELECT id, name, country, sector, url, owner, uid, kind, asset_type, version_id, date_created FROM {tableName} WHERE id= '{project_id}'"
            project_result = read_query(query)
            # Build generic metadata
            generic_metadata = {}
            generic_metadata['name'] = project_result[0][2]
            generic_metadata['country'] = project_result[0][3]
            generic_metadata['sector'] = project_result[0][4]

            ASSET_UID = project_result[0][6]
            URL = f'https://kf.kobotoolbox.org/api/v2/assets/{ASSET_UID}/data/'
            params = {
                'format': 'json'
            }
            rawResult = kobo_api(URL, params)
            data = json.loads(rawResult.content.decode('utf-8'))
            data = data['results']
            i = 1
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
                                i += 1
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
            transaction_id = insert_metadata(data_id, main_metadata)
        ## TODO: Add metadata by project into the database records
        print(main_metadata)
        
    else:
        print("No data to process in the blockchain")
except AssertionError:
    print("Problem with some of the documents. No ipfs hash available")