"""
Purpose of the script is to read structure from json file and convert into  an SQLite database with linked Tables and
attributes.
"""

import json
import logging
import os
from lib import ansiblestore
from lib import my_env


def get_attributes(table_pair, parent_table=None):
    """
    This module will get the attributes from a table, then create the table in sqlite. Attributes come in pairs
    (ansible_id, sql_field).

    :param table_pair: pair of (ansible_id, sql_table).
    :param parent_table: table name of the parent table where current table links to.
    :return:
    """
    table_name = table_pair[1]
    attrib_pairs = attribs_table[table_name]
    attribs = [attrib[1] for attrib in attrib_pairs]
    # Remove duplicate field names
    attribs = list(set(attribs))
    if len(attribs) > 0:
        cols = ' text,'.join(attribs)
        col_str = f"id integer PRIMARY KEY AUTOINCREMENT, {cols} text"
    else:
        col_str = f"id integer PRIMARY KEY AUTOINCREMENT"
    if parent_table:
        parent_col = f"{parent_table}_id"
        fk_str = f", {parent_col} int, FOREIGN KEY ({parent_col}) REFERENCES {parent_table} (id)"
    else:
        fk_str = ""
    create_str = f"CREATE TABLE {table_name} ({col_str} {fk_str});"
    logging.info(create_str)
    ans.run_query(create_str)
    # Now get child-tables linking to this table.
    get_tables(table_name)
    return


def get_tables(parent_table):
    """
    This script will get all tables linked to a parent table.

    :param parent_table:
    :return:
    """
    for table_name in linked_tables[parent_table]:
        get_attributes(table_name, parent_table)
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
ans = ansiblestore.SqliteUtils()

ans.rebuild()
# Collect structure first: attributes related to tables, and how tables are linked to each other.
# Key is the unique table name, value is a list of dictionary attributes
struct_json = os.path.join(os.getenv('STRUCTDIR'), os.getenv('STRUCTFILE'))
with open(struct_json) as struct_file:
    data = json.loads(struct_file.read())

linked_tables = data['tables']
attribs_table = data['attribs']
get_attributes(['server', 'server'])

logging.info("End Application")
