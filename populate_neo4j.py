"""
Purpose of the script is to get a JSON file and extract the structure of it into Neo4J database.
Check for git.
"""

import json
from lib import my_env
from lib.my_env import *
from lib import neostore

attribs = {}
has_attrib = 'has_attrib'


def handle_dict(diction, parent):
    """
    This function handles a dictionary.

    :param diction: dictionary to handle
    :param parent: Parent node for the connections.
    :return:
    """
    for k in diction:
        # node = my_env.clean(k)
        if not in_attrib_stop(k):
            # node is attribute attached to parent
            # Create node if it does not exist already
            try:
                attrib_lbl = attrib_sub[parent['name']]
            except KeyError:
                attrib_lbl = k
            attrib_name = "{}*{}".format(parent['attrib'], attrib_lbl)
            if attrib_name not in attribs:
                # attribs.append(attrib_name)
                lbl = k
                props = dict(name=lbl, attrib=attrib_name)
                this_node = ns.create_node(k, **props)
                attribs[attrib_name] = this_node
                ns.create_relation(parent, has_attrib, this_node)
            if isinstance(diction[k], dict):
                parent_node = attribs[attrib_name]
                handle_dict(diction[k], parent_node)
            elif isinstance(diction[k], list):
                for elem in diction[k]:
                    if isinstance(elem, dict):
                        parent_node = attribs[attrib_name]
                        handle_dict(elem, parent_node)
            # To do: Handle case where diction[k] is list of strings (not a list of dictionaries.
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ns = neostore.NeoStore(refresh='Yes')

with open(cmdb_json) as cmdb_file:
    data = json.loads(cmdb_file.read())
    li = my_env.LoopInfo('Servers', 10000)
    # Create Server Node
    t_lbl = 'server'
    t_props = dict(name=t_lbl, attrib=t_lbl)
    top_node = ns.create_node(t_lbl, **t_props)
    attribs[t_lbl] = top_node
    for item in data:
        handle_dict(data[item], top_node)
print(f"Found {li.end_loop()} servers.")
for attrib in attribs:
    print(f"Server attribute: {attrib}")
logging.info("End Application")
