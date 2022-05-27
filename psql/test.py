
from utils import confirm_transaction
import time
from CardanoPython.src.cardano import base
node = base.Node()

master_address = 'MasterGetFair'

result = node.get_transactions(master_address)
print(result['transactions'][0]['hash'])

TxHash = '77ba58633fa5f18efebb7ae8762ea2a1a0082d353af289929bfc87eaabbc62a2'

transaction_result = confirm_transaction(TxHash)
while transaction_result['hash'] != TxHash:  # type: ignore
    time.sleep(5)
    print(1)
    transaction_result = confirm_transaction(TxHash)