import requests
from requests.structures import CaseInsensitiveDict
from decouple import config
from pinata import PinataPy
from blockfrost import BlockFrostApi, ApiError, ApiUrls

def kobo_api(URL, params= {}):
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "Token XXXXXXXXXXXX"

    resp = requests.get(URL, headers=headers, params=params)
    rawResult = resp
    return rawResult

def ipfs(file_path_name):
    #Inputs
    PINATA_API_KEY= config('PINATA_API_KEY')
    PINATA_SECRET_API_KEY = config('PINATA_SECRET_API_KEY')

    c = PinataPy(PINATA_API_KEY,PINATA_SECRET_API_KEY)
    cid = c.pin_file_to_ipfs(file_path_name)
    print(f"IPFS hash is: {cid}")
    return cid

def confirm_transaction(hash):

    api = BlockFrostApi(
        project_id=config('BLOCKFROST_API_KEY'),  # or export environment variable BLOCKFROST_PROJECT_ID
        # optional: pass base_url or export BLOCKFROST_API_URL to use testnet, defaults to ApiUrls.mainnet.value
        base_url=ApiUrls.testnet.value,
    )
    try:
        transaction = api.transaction(hash=hash, return_type='json')
        return transaction

    except ApiError as e:
        print(e)
        return e


"""
Activate when running manually
"""

if __name__ == '__main__':
    pass
