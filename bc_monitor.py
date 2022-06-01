#!/usr/bin/python
from dblib import read_query, write_query
import json
from psycopg2.extras import Json
from CardanoPython.src.cardano import base
from datetime import datetime
import time

def build_metadata(data_result):

    picture_id = None
    try:
        main_metadata = {}
        # Get the project UID
        data_id = data_result[0]
        project_id = data_result[1]
        _id = data_result[2]
        uuid = data_result[3]
        ubicacion_usuario = data_result[4]
        usuario = data_result[5]
        cargo_usuario = data_result[6]
        departamento_ubicacion = data_result[7]
        municipio_ubicacion = data_result[8]
        submission = data_result[9]
        # verify if metadata is already in transactions table
        tableName = 'transactions'
        query = f"SELECT index, metadata FROM {tableName} WHERE data_id = '{data_id}';"
        transaction_row = read_query(query)
        if not transaction_row:

            # Build metadata
            data_metadata = {}
            data_metadata['uuid'] = uuid
            data_metadata['usuario'] = usuario
            data_metadata['cargo_usuario'] = cargo_usuario
            data_metadata['departamento_ubicacion'] = departamento_ubicacion
            data_metadata['municipio_ubicacion'] = municipio_ubicacion
            data_metadata['ubicacion_usuario'] = ubicacion_usuario
            data_metadata['submission'] = datetime.strftime(submission,"%Y-%m-%dT%H:%M:%S")
            # verify if metadata is already in transactions table
            tableName = 'measurement'
            query = f"SELECT measurement, value, picture_id FROM {tableName} WHERE project_id = '{project_id}' and _id = '{_id}';"
            measurement_row = read_query(query)

            for measurement in measurement_row:
                meas = measurement[0]
                value = measurement[1]
                picture_id = measurement[2]
                # Get the IPFS hash link
                tableName = 'pictures'
                query = f"SELECT ipfshash FROM {tableName} WHERE project_id = '{project_id}' and data_id = '{data_id}' and picture_id='{picture_id}';"
                ipfshash = read_query(query)
                assert ipfshash is not []
                data_metadata[meas] = str(float(value))
                data_metadata[str(meas) + '_file'] = ipfshash[0][0]

            tableName = 'projects'
            query = f"SELECT id, name, country, sector, url, owner, uid, kind, asset_type, version_id, date_created FROM {tableName} WHERE id= '{project_id}'"
            project_result = read_query(query)
            if project_result is not None:
                # Build generic metadata
                generic_metadata = {}
                generic_metadata['name'] = project_result[0][1]
                generic_metadata['country'] = project_result[0][2]
                generic_metadata['sector'] = project_result[0][3]
                generic_metadata['kind'] = project_result[0][7]
                generic_metadata['asset_type'] = project_result[0][8]
                main_metadata = {
                    "294857930485": {
                        "project_info": generic_metadata,
                        "data_info": data_metadata
                    }
                }
        else:
            main_metadata = transaction_row[0][1]
        return main_metadata
        
    except AssertionError:
        print(f"Problem with some of the documents. No ipfs hash available for picture_id: {picture_id}")

def transaction_build(master_address, metadata, data_id):
    node = base.Node()
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
    query = f"UPDATE {tableName} SET address_origin = '{master_address}', txin = '{tx_utxo_input}', tx_cborhex = {Json(cbor_hex)}, fees='{fees}', network='{network}'  where data_id='{data_id}' returning index;"
    transaction_id = write_query(query)
    node.sign_transaction(master_address)
    node.submit_transaction()
    TxHash = node.get_txid()
    TxHash = TxHash[:-1]
    # Store the transaction result in database
    tableName = 'transactions'
    query = f"UPDATE {tableName} SET tx_hash = '{TxHash}' where index='{transaction_id}' returning index;"
    transaction_id = write_query(query)
    # Wait to confirm the transaction in the blockchain
    transaction_result = node.get_transactions(master_address)
    
    while transaction_result['transactions'][0]['hash'] != TxHash:
        time.sleep(30)
        print(time.time())
        transaction_result = node.get_transactions(master_address)
    tableName = 'transactions'
    query = f"UPDATE {tableName} SET processed = 'True' where index='{transaction_id}' returning index;"
    transaction_id = write_query(query)
    print(f"transaction succesfully submitted with Hash: {TxHash} in table 'data' with id: {data_id} and 'table' transactions with index: {transaction_id}")
    return params


if __name__ == '__main__':

    try:
        # Check if transaction results are pending to be sent to the blockchain
        tableName = 'data'
        query = f"SELECT id, project_id, _id, _uuid, gpslocation, usuario, role, dlocation, mlocation, submission FROM {tableName} WHERE validation = 'Approved';"
        data_results = read_query(query)
        if data_results != [] or data_results is not None:
            for data_result in data_results:
                data_id = data_result[0]
                 # Compare if data record is already created as metadata
                tableName = 'transactions'
                query = f"SELECT processed FROM {tableName} WHERE data_id='{data_id}';"
                processed_from_transactions = read_query(query)
                master_address = 'MasterGetFair'
                if processed_from_transactions != []:
                    if processed_from_transactions[0][0] != True:
                        # Build metadata 
                        metadata = build_metadata(data_result)
                        tableName = 'transactions'
                        query = f"UPDATE {tableName} SET metadata={Json(metadata)} WHERE data_id = '{data_id}' RETURNING index;"
                        transaction_id = write_query(query)
                        params = transaction_build(master_address, metadata, data_id)
                    else:
                        print("No data to process in the blockchain")
                else:
                    # Build metadata 
                    metadata = build_metadata(data_result)
                    tableName = 'transactions'
                    query = f"INSERT INTO {tableName} (data_id, metadata)\nVALUES ({str(data_id)}, {Json(metadata)}) RETURNING index;"
                    transaction_id = write_query(query)
                # Time to build the transaction draft
                    tableName = 'transactions'
                    query = f"SELECT metadata FROM {tableName} WHERE data_id = '{data_id}';"
                    metadata = read_query(query)
                    params = transaction_build(master_address, metadata[0][0], data_id)
        else:
            print("No data to process in the blockchain")

    except TypeError:
        print("No projects found in table projects")