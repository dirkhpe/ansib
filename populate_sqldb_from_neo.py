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
    :param this_table: Unique name of the current table (not the name of the dictionary)
    :param table_props: Properties of the parent table (name and ID of last inserted record).
    :return:
    """
    row_dict = {}
    attribs = attribs_table[this_table]
    for attrib in attribs:
        # logging.info(f"Evaluating {attrib}")
        try:
            if isinstance(diction[attrib], list):
                # print(f"Evaluating: {diction[attrib]}")
                row_dict[clean(attrib)] = ", ".join(str(v) for v in diction[attrib])
            else:
                row_dict[clean(attrib)] = diction[attrib]
        except KeyError:
            pass
    # Now add link to parent table in current record. Field name is in table_attrib, field value is in table_id.
    try:
        row_dict[table_props['table_attrib']] = table_props['table_id']
    except KeyError:
        pass
    table_id = ans.insert_row(this_table, row_dict)
    table_attrib = f"{this_table}_id"
    props = dict(table_id=table_id, table=this_table, table_attrib=table_attrib)
    tables = linked_tables[this_table]
    for table in tables:
        unique_tn = table["unique_tn"]
        attrib = table['table']
        try:
            if isinstance(diction[attrib], dict):
                handle_dict(diction[attrib], unique_tn, **props)
            elif isinstance(diction[attrib], list):
                for elem in diction[attrib]:
                    if isinstance(elem, dict):
                        handle_dict(elem, unique_tn, **props)
                    else:
                        logging.debug(f"Dictionary expected for {attrib}, found {type(diction[attrib])} "
                                      f"containing {diction[attrib]}")
        except KeyError:
            pass
    return


def get_attributes(sn):
    """
    This module will get the attributes from a table and add the attributes as a list to the table.

    :param sn: Start node for the table.
    :return:
    """
    table_name = ns.get_unique_tn(sn)
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
    parent_table = ns.get_unique_tn(sn)
    tables = []
    cursor = ns.get_tables(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        table = next(iter(node.labels))
        unique_tn = ns.get_unique_tn(node)
        props = dict(table=table, unique_tn=unique_tn)
        tables.append(props)
        get_attributes(node)
    linked_tables[parent_table] = tables
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ans = ansiblestore.SqliteUtils()
ns = neostore.NeoStore(refresh='No')

# Collect structure first: attributes related to tables, and how tables are linked to each other.
# Key is the unique table name, value is a list of dictionary attributes
attribs_table = {}
# Key is unique table name, value is dictionary with table (dictionary table name) and unique_tn (unique table name)
linked_tables = {}
start_node = ns.get_nodes('server')[0]
get_attributes(start_node)

with open(cmdb_json) as cmdb_file:
    data = json.loads(cmdb_file.read())
    li = my_env.LoopInfo('Servers', 20)
    # Create Server Node
    t_name = 'server'
    for item in data:
        li.info_loop()
        logging.info(f"Working on server {item}")
        handle_dict(data[item], t_name)
    li.end_loop()
logging.info("End Application")
