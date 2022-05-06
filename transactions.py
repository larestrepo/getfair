from CardanoPython.src.cardano import base
import json

node = base.Node()
# result = node.query_protocol()
# print(result)
# result = node.query_tip_exec()
# print(result)

address_origin = 'addr_test1qpr9xmkn4fpexdcd9e8kt8fektvqyrg3vetmw8pmmlava2kcyxgcpfar5pu5dlxx9y9c0mm2mtj48uz56q9aakvn2vksw633r5'

balance = node.get_balance(address_origin)

with open('./metadata.json', 'r') as file:
    metadata = json.load(file)

witness = 1

params = {
  "message": {
    "tx_info": {
      "address_origin": [
        address_origin,
        ],
        "address_destin":None,
      "change_address": address_origin,
      "metadata": metadata,
      "mint": None,
      "script_path": None,
      "witness": witness,
    }
  }
}
node.build_tx_components(**params)