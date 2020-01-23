"""
Purpose of the script is to populate the sqlite ansible database.
"""

import json
from lib import ansiblestore
from lib import my_env
from lib.my_env import *
from lib import neostore


def handle_dict(diction, this_table, **table_props):
    """
    This function handles a dictionary. First collect all attributes for the table and add values as row in the
    database. Then walk through all linked tables.

    :param diction: dictionary to handle
    :param this_table: Name of the current table
    :param table_props: Properties of the parent table (name and ID of last inserted record).
    :return:
    """
    row_dict = {}
    attribs = attribs_table[this_table]
    for attrib in attribs:
        try:
            if isinstance(diction[attrib], list):
                # print(f"Evaluating: {diction[attrib]}")
                row_dict[clean(attrib)] = ", ".join(str(v) for v in diction[attrib])
            else:
                row_dict[clean(attrib)] = diction[attrib]
        except KeyError:
            pass
    try:
        row_dict[table_props['table_attrib']] = table_props['table_id']
    except KeyError:
        pass
    table_id = ans.insert_row(clean(this_table), row_dict)
    table_attrib = f"{clean(this_table)}_id"
    props = dict(table_id=table_id, table=this_table, table_attrib=table_attrib)
    tables = linked_tables[this_table]
    for table in tables:
        try:
            handle_dict(diction[table], table, **props)
        except KeyError:
            pass
    return


def get_attributes(sn, parent_table=None):
    """
    This module will get the attributes from a table and add the attributes as a list to the table.

    :param sn: Start node for the table.
    :param parent_table: table name of the parent table where current table links to.
    :return:
    """
    table_name = next(iter(sn.labels))
    attribs = []
    cursor = ns.get_attribs(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        attrib = next(iter(node.labels))
        attribs.append(attrib)
    attribs_table[table_name] = attribs
    # Now get child-tables linking to this table.
    get_tables(sn)
    return


def get_tables(sn):
    """
    This script will get all tables linked to a parent table.

    :param sn: Start node
    :return:
    """
    parent_table = next(iter(sn.labels))
    tables = []
    cursor = ns.get_tables(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        table = next(iter(node.labels))
        tables.append(table)
        get_attributes(node, parent_table)
    linked_tables[parent_table] = tables
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ans = ansiblestore.SqliteUtils()
ns = neostore.NeoStore(refresh='No')

# Collect structure first: attributes related to tables,
# and how tables are linked to each other.

attribs_table = {}
linked_tables = {}

start_node = ns.get_nodes('server')[0]
get_attributes(start_node)

with open(cmdb_json) as cmdb_file:
    data = json.loads(cmdb_file.read())
    li = my_env.LoopInfo('Servers', 10)
    # Create Server Node
    t_name = 'server'
    for item in data:
        handle_dict(data[item], t_name)
print(f"Found {li.end_loop()} servers.")
logging.info("End Application")
