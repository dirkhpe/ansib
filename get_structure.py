"""
Purpose of the script is to extract structure from Neo4J DB and convert into Linked Tables and attributes.
"""

import logging
from lib import ansiblestore
from lib import my_env
from lib import neostore


def get_attributes(sn, parent_table=None):
    """
    This module will get the attributes from a table, then create the table in sqlite.

    :param sn: Start node for the table.
    :param parent_table: table name of the parent table where current table links to.
    :return:
    """
    table_name = my_env.clean(next(iter(sn.labels)))
    attribs = []
    cursor = ns.get_attribs(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        attrib = my_env.clean(next(iter(node.labels)))
        if attrib not in attribs:
            attribs.append(attrib)
    cols = ' text,'.join(attribs)
    col_str = f"id integer PRIMARY KEY AUTOINCREMENT, {cols} text"
    if parent_table:
        parent_col = f"{parent_table}_id"
        fk_str = f", {parent_col} int, FOREIGN KEY ({parent_col}) REFERENCES {parent_table} (id)"
    else:
        fk_str = ""
    create_str = f"CREATE TABLE {table_name} ({col_str} {fk_str});"
    logging.info(create_str)
    ans.run_query(create_str)
    # Now get child-tables linking to this table.
    get_tables(sn)
    return


def get_tables(sn):
    """
    This script will get all tables linked to a parent table.

    :param sn: Start node
    :return:
    """
    parent_table = my_env.clean(next(iter(sn.labels)))
    cursor = ns.get_tables(sn)
    while cursor.forward():
        rec = cursor.current
        node = rec['end_node']
        get_attributes(node, parent_table)
    return


# Initialize Environment
projectname = "ansible"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
ns = neostore.NeoStore(refresh='No')
ans = ansiblestore.SqliteUtils()

ans.rebuild()

start_node = ns.get_nodes('server')[0]
get_attributes(start_node)

logging.info("End Application")
