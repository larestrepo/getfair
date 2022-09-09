from CardanoPythonLib.cardanopythonlib import base

config_path = './cardano_config.ini' # Optional argument
node = base.Node(config_path) # Or with the default ini: node = base.Node()
print(node.CARDANO_ERA)
node.query_tip_exec()