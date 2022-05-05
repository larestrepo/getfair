import os
from decouple import config
from pinata import PinataPy

"""
1. Loads picture in IPFS
"""

#Inputs
PINATA_API_KEY= config('PINATA_API_KEY')
PINATA_SECRET_API_KEY = config('PINATA_SECRET_API_KEY')
path = ('/file_example/Image1.png') # place of the images to be uploaded

dirname = os.path.dirname(__file__)
path = dirname + path
c = PinataPy(PINATA_API_KEY,PINATA_SECRET_API_KEY)
cid = c.pin_file_to_ipfs(path)
print(cid)