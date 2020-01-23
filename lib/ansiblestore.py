"""
Script to consolidate SQLite functions to store ansible data.
"""

import logging
import os
import sqlite3


class SqliteUtils:
    """
    This class consolidates a number of Database utilities for sqlite, such as rebuild of the database or rebuild of a
    specific table.
    """

    def __init__(self):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = os.path.join(os.getenv("DBDIR"), os.getenv("DBNAME"))
        self.dbConn, self.cur = self._connect2db()

    def _connect2db(self):
        """
        Internal method to create an sqlite database connection.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Connection and cursor to the sqlite database.
        """
        db_conn = sqlite3.connect(self.db)
        db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def close(self):
        self.dbConn.close()

    def get_query(self, query):
        """
        This method will get a query and return the result of the query.

        :param query:
        :return:
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def get_table(self, tablename):
        """
        This method will return the table as a list of named rows. This means that each row in the list will return
        the table column values as an attribute. E.g. row.name will return the value for column name in each row.

        :param tablename:
        :return:
        """
        query = "SELECT * FROM {t}".format(t=tablename)
        self.cur.execute(query)
        res = self.cur.fetchall()
        return res

    def insert_row(self, tablename, rowdict):
        """
        This method will insert a dictionary row into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: Row Dictionary
        :return: Row ID of the last row inserted.
        """
        columns = ", ".join("`" + k + "`" for k in rowdict.keys())
        values_template = ", ".join(["?"] * len(rowdict.keys()))
        query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
        values = tuple(rowdict[key] for key in rowdict.keys())
        logging.debug("Insert query: {q}".format(q=query))
        self.cur.execute(query, values)
        self.dbConn.commit()
        return self.cur.lastrowid

    def insert_rows(self, tablename, rowdict):
        """
        This method will insert a list of dictionary rows into a table.

        :param tablename: Table Name to insert data into
        :param rowdict: List of Dictionary Rows
        :return:
        """
        if len(rowdict) > 0:
            columns = ", ".join("`" + k + "`" for k in rowdict[0].keys())
            values_template = ", ".join(["?"] * len(rowdict[0].keys()))
            query = "insert into {tn} ({cols}) values ({vt})".format(tn=tablename, cols=columns, vt=values_template)
            logging.debug("Insert query: {q}".format(q=query))
            # cnt = my_env.LoopInfo(tablename, 50)
            for line in rowdict:
                # cnt.info_loop()
                logging.debug(line)
                values = tuple(line[key] for key in line.keys())
                try:
                    self.dbConn.execute(query, values)
                except sqlite3.IntegrityError:
                    logging.error("Integrity Error on query {q} with values {v}".format(q=query, v=values))
                except sqlite3.InterfaceError:
                    logging.error("Interface error on query {q} with values {v}".format(q=query, v=values))
            # cnt.end_loop()
            self.dbConn.commit()
        return

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        if self.dbConn:
            self.dbConn.close()
            os.remove(self.db)
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()

    def run_query(self, query):
        """
        Run a query that does not return results, such as create a table or remove a table. Execute the query in a
        cursor, then commit the change.

        :return:
        """
        self.cur.execute(query)
        self.dbConn.commit()
        return
