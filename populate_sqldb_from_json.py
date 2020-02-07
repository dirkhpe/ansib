"""
Purpose of the script is to populate the sqlite ansible database.
"""

import json
from lib import ansiblestore
from lib import my_env
from lib.my_env import *


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
    for ansible_id, sql_field in attribs:
        logging.debug(f"Evaluating {ansible_id}")
        try:
            if isinstance(diction[ansible_id], list):
                row_dict[sql_field] = ", ".join(str(v) for v in diction[ansible_id])
            else:
                row_dict[sql_field] = diction[ansible_id]
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
    for ansible_id, sql_table in tables:
        try:
            if isinstance(diction[ansible_id], dict):
                handle_dict(diction[ansible_id], sql_table, **props)
            elif isinstance(diction[ansible_id], list):
                for elem in diction[ansible_id]:
                    if isinstance(elem, dict):
                        handle_dict(elem, sql_table, **props)
                    else:
                        logging.debug(f"Dictionary expected for {ansible_id}, found {type(diction[ansible_id])} "
                                      f"containing {diction[ansible_id]}")
        except KeyError:
            pass
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
cmdb_json = os.getenv('JSONFILE')
ans = ansiblestore.SqliteUtils()

# Collect structure first: attributes related to tables, and how tables are linked to each other.
# Key is the unique table name, value is a list of dictionary attributes
struct_json = os.path.join(os.getenv('STRUCTDIR'), os.getenv('STRUCTFILE'))
with open(struct_json) as struct_file:
    data = json.loads(struct_file.read())

linked_tables = data['tables']
attribs_table = data['attribs']

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
