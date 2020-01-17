"""
Purpose of the script is to get a JSON file and extract the structure of it.
"""

import json
import logging
import os
from lib import my_env
from lib import neostore

server_attribs = []


def handle_dict(diction, parent):
    """
    This function handles a dictionary.

    :param diction: dictionary to handle
    :param parent: Label of the parent of the dictionary.
    :return:
    """
    for k in diction:
        if isinstance(k, str):
            if k not in server_attribs:
                server_attribs.append(k)
                props = dict(name=k)
                ns.create_node('Server', **props)


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ns = neostore.NeoStore()

with open(cmdb_json) as cmdb_file:
    data = json.loads(cmdb_file.read())
    li = my_env.LoopInfo('Servers', 10000)
    for item in data:
        handle_dict(data[item], 'server')
print(f"Found {li.end_loop()} servers.")
server_attribs.sort()
for attrib in server_attribs:
    print(f"Server attribute: {attrib}")
logging.info("End Application")
