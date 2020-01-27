"""
Purpose of the script is to show linked tables.
"""

from lib import my_env
from lib.my_env import *
from lib import neostore


def get_attributes(sn):
    """
    This module will get the attributes from a table and add the attributes as a list to the table.

    :param sn: Start node for the table.
    :return:
    """
    get_tables(sn)
    return


def get_tables(sn):
    """
    This script will get all tables linked to a parent table.

    :param sn: Start node
    :return:
    """
    parent_table = ns.get_unique_tn(sn)
    tables = []
    cursor = ns.get_tables(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        table = ns.get_unique_tn(node)
        tables.append(table)
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

start_node = ns.get_nodes('server')[0]
get_attributes(start_node)

for item in sorted(linked_tables.keys()):
    print(f"{item}: {linked_tables[item]}")
logging.info("End Application")
