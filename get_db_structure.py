"""
Purpose of the script is to show linked tables.
"""

import json
from lib import my_env
from lib.my_env import *
from lib import neostore


def get_attributes(sn):
    """
    This module will get the attributes from a table, then create the table in sqlite. Table attributes are in pairs
    (ansible_id, sql_field).

    :param sn: Start node for the table.
    :return:
    """
    table_name = ns.get_unique_tn(sn)
    attribs = []
    cursor = ns.get_attribs(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        ansible_id = next(iter(node.labels))
        sql_field = my_env.clean(ansible_id)
        attrib = (ansible_id, sql_field)
        if attrib not in attribs:
            attribs.append(attrib)
    table_attribs[table_name] = attribs
    # Now get child-tables linking to this table.
    get_tables(sn)
    return


def get_tables(sn):
    """
    This script will get all tables linked to a parent table. Table names are in pairs (ansible_id, sql_table). Key of
    the dictionary is sql_table since this is unique and formatted for SQL.

    :param sn: Start node
    :return:
    """
    parent_table = ns.get_unique_tn(sn)
    tables = []
    cursor = ns.get_tables(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        ansible_id = next(iter(node.labels))
        sql_table = ns.get_unique_tn(node)
        tables.append([ansible_id, sql_table])
        get_attributes(node)
    linked_tables[parent_table] = tables
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ns = neostore.NeoStore(refresh='No')

# Collect structure how tables are linked to each other.

linked_tables = {}
table_attribs = {}

start_node = ns.get_nodes('server')[0]
get_attributes(start_node)

db_struct = dict(tables=linked_tables, attribs=table_attribs)
struct_file = os.path.join(os.getenv('STRUCTDIR'), os.getenv('STRUCTFILE'))
fh = open(struct_file, 'w')
fh.write(json.dumps(db_struct, indent=4))
fh.close()
logging.info("End Application")
