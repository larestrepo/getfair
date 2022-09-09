#!/usr/bin/python
from dblib import read_query, write_query
import json
from psycopg2.extras import Json
from datetime import datetime
import time
from CardanoPythonLib.cardanopythonlib import base

node = base.Node('./cardano_config.ini')

def build_metadata(data_result):

    picture_id = None
    main_metadata = {}
    # Get the project UID
    data_id = data_result[0]
    # verify if metadata is already in transactions table
    tableName = 'transactions'
    query = f"SELECT index, metadata FROM {tableName} WHERE data_id = '{data_id}';"
    transaction_row = read_query(query)
    if not transaction_row:
        # Build metadata
        project_id = data_result[1]
        _id = data_result[2]
        data_metadata = data_result[3]
        data_metadata['_id'] = _id

        # verify if metadata is already in transactions table
        tableName = 'measurement'
        query = f"SELECT measurement, value, picture_id FROM {tableName} WHERE project_id = '{project_id}' and _id = '{_id}';"
        measurement_row = read_query(query)
        for measurement in measurement_row:
            meas = measurement[0]
            value = measurement[1]
            picture_id = measurement[2]
            if picture_id is not None:
            # Get the IPFS hash link
                tableName = 'pictures'
                query = f"SELECT ipfshash FROM {tableName} WHERE project_id = '{project_id}' and data_id = '{data_id}' and picture_id={picture_id};"
                ipfshash = read_query(query)
                if ipfshash != []:
                    data_metadata[str(meas) + '_file'] = ipfshash[0][0]
            data_metadata[meas] = str(value)

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
        
    # except AssertionError:
    #     print(f"Problem with some of the documents. No ipfs hash available for picture_id: {picture_id}")

def transaction_build(master_address, metadata, data_id):
    estimated_fees = None
    try:
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
        estimated_fees = node.build_tx_components(params)
        fees = 0
        assert estimated_fees != None
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
        return transaction_id

    except AssertionError:
        print("Errors while building the transaction.") 
        return None


def monitor_transaction(transaction_id, address):

    TxHash = node.get_txid()
    TxHash = TxHash[:-1]
    # Store the transaction result in database
    tableName = 'transactions'
    query = f"UPDATE {tableName} SET tx_hash = '{TxHash}' where index='{transaction_id}' returning index;"
    transaction_id = write_query(query)
    # Wait to confirm the transaction in the blockchain
    transaction_result = node.get_transactions(address)
    confirmation = False
    while not confirmation:  # type: ignore
        for transaction in transaction_result:
            if transaction['hash'] == TxHash:
                confirmation = True 
        time.sleep(10)
        transaction_result = node.get_transactions(address)
    tableName = 'transactions'
    query = f"UPDATE {tableName} SET processed = 'True' where index='{transaction_id}' returning index;"
    transaction_id = write_query(query)
    print(f"transaction succesfully submitted with Hash: {TxHash} in table 'data' with id: {data_id} and 'table' transactions with index: {transaction_id}")
    

if __name__ == '__main__':
    store_only = False
    sign_name = 'MasterGetFair'
    master_address = 'addr_test1qpr9xmkn4fpexdcd9e8kt8fektvqyrg3vetmw8pmmlava2kcyxgcpfar5pu5dlxx9y9c0mm2mtj48uz56q9aakvn2vksw633r5'
    # try:
    # Check if transaction results are pending to be sent to the blockchain
    tableName = 'data'
    # query = f"SELECT id, project_id, _id, _uuid, gpslocation, usuario, role, dlocation, mlocation, submission FROM {tableName} WHERE validation = 'Approved';"
    query = f"SELECT id, project_id, _id, json FROM {tableName} WHERE validation = 'Approved';"
    data_results = read_query(query)
    if data_results != [] or data_results is not None:
        for data_result in data_results:
            data_id = data_result[0]
                # Compare if data record is already created as metadata
            tableName = 'transactions'
            query = f"SELECT processed FROM {tableName} WHERE data_id='{data_id}';"
            processed_from_transactions = read_query(query)
            if processed_from_transactions != []:
                if processed_from_transactions[0][0] != True:
                    # Build metadata 
                    metadata = build_metadata(data_result)
                    tableName = 'transactions'
                    query = f"UPDATE {tableName} SET metadata={Json(metadata)}, processed = False WHERE data_id = '{data_id}' RETURNING index;"
                    transaction_id = write_query(query)
                    if not store_only:
                        transaction_id = transaction_build(master_address, metadata, data_id)
                        if transaction_id is not None:
                            node.sign_transaction(sign_name)
                            node.submit_transaction()
                            monitor_transaction(transaction_id, master_address)
                else:
                    print("No data to process in the blockchain")
            else:
                # Build metadata 
                metadata = build_metadata(data_result)
                tableName = 'transactions'
                query = f"INSERT INTO {tableName} (data_id, metadata, processed)\nVALUES ({str(data_id)}, {Json(metadata)}, False) RETURNING index;"
                transaction_id = write_query(query)
                if not store_only:
                    transaction_id = transaction_build(master_address, metadata, data_id)
                    if transaction_id is not None:
                        node.sign_transaction(sign_name)
                        node.submit_transaction()
                        monitor_transaction(transaction_id, master_address)
    else:
        print("No data to process in the blockchain")

    # except TypeError:
    #     print("No projects found in table projects")