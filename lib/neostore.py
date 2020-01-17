""""
This class consolidates functions related to the neo4J datastore.
"""

import logging
import os
import uuid
from py2neo import Database, Graph, Node, Relationship, NodeMatcher, RelationshipMatch


class NeoStore:

    def __init__(self, refresh='No'):
        """
        Method to instantiate the class in an object for the neostore module.

        :param refresh: Database refresh required? (No: default)
        """
        logging.debug("Initializing Neostore object")
        self.graph = self._connect2db()
        if refresh == 'Yes':
            self._delete_all()
        self.nodematcher = NodeMatcher(self.graph)

    @staticmethod
    def _connect2db():
        """
        Internal method to create a database connection. This method is called during object initialization.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Neostore object.")
        neo4j_config = {
            'user': os.getenv("NEO4J_USER"),
            'password': os.getenv("NEO4J_PWD"),
        }
        # Check that Neo4J is running the expected Neo4J Store - to avoid accidents...
        connected_db = Database(**neo4j_config)
        dbname = connected_db.config["dbms.active_database"]
        if dbname != os.getenv("NEO4J_DB"):
            msg = "Connected to Neo4J database {d}, but expected to be connected to {n}"\
                .format(d=dbname, n=os.getenv("NEO4J_DB"))
            logging.fatal(msg)
            raise SystemExit(msg)
        # Connect to Graph
        graph = Graph(**neo4j_config)
        # Check that we are connected to the expected Neo4J Store - to avoid accidents...
        return graph

    def get_endnode(self, start_node=None, rel_type=None):
        """
        This method will calculate the end node from a start Node and a relation type. If relation type is not specified
        then any relation type will do.
        The purpose of the function is to find a single end node. If there are multiple end nodes, then a random one
        is returned and an error message will be displayed.

        :param start_node: Start node.
        :param rel_type: Relation type
        :return: End Node, or False.
        """
        if not isinstance(start_node, Node):
            logging.error("Attribute not type Node (instead type {t})".format(t=type(start_node)))
            return False
        rels = RelationshipMatch(self.graph, (start_node, None), r_type=rel_type)
        if rels.__len__() == 0:
            logging.warning("No end node found for start node ID: {nid} and relation: {rel}"
                            .format(nid=start_node["nid"], rel=rel_type))
            return False
        elif rels.__len__() > 1:
            logging.warning("More than one end node found for start node ID {nid} and relation {rel},"
                            " returning first".format(nid=start_node["nid"], rel=rel_type))
        return rels.first().end_node

    def get_endnodes(self, start_node=None, rel_type=None):
        """
        This method will calculate all end nodes from a start Node and a relation type. If relation type is not
        specified then any relation type will do.
        The purpose of the function is to find all end nodes.
        :param start_node: Start node.
        :param rel_type: Relation type
        :return: List with End Nodes.
        """
        if not isinstance(start_node, Node):
            logging.error("Attribute not type Node (instead type {t})".format(t=type(start_node)))
            return False
        node_list = [rel.end_node
                     for rel in RelationshipMatch(self.graph, (start_node, None), r_type=rel_type)]
        # Convert to set to remove duplicate end nodes
        node_set = set(node_list)
        # Then return the result as a list
        return list(node_set)

    def create_node(self, *labels, **props):
        """
        Function to create node. The function will return the node object.
        :param labels: Labels for the node
        :param props: Value dictionary with values for the node.
        :return: node object
        """
        logging.debug("Trying to create node with params {p}".format(p=props))
        props['nid'] = str(uuid.uuid4())
        component = Node(*labels, **props)
        self.graph.create(component)
        return component

    def create_relation(self, from_node, rel, to_node):
        """
        Function to create relationship between nodes. If the relation exists already, it will not be created again.
        :param from_node:
        :param rel:
        :param to_node:
        :return:
        """
        rel = Relationship(from_node, rel, to_node)
        self.graph.merge(rel)
        return

    def _delete_all(self):
        """
        Function to remove all nodes and relations from the graph database.
        Then create calendar object.
        :return:
        """
        logging.info("Remove all nodes and relations from database.")
        self.graph.delete_all()
        return

    def get_nodes(self, *labels, **props):
        """
        This method will select all nodes that have labels and properties
        :param labels:
        :param props:
        :return: list of nodes that fulfill the criteria, or False if no nodes are found.
        """
        nodes = self.nodematcher.match(*labels, **props)
        nodelist = list(nodes)
        if len(nodelist) == 0:
            # No nodes found that fulfil the criteria
            return False
        else:
            return nodelist

    def get_query(self, query, **kwargs):
        """
        This method accepts a Cypher query and returns the result as a cursor.
        :param query: Cypher Query to run
        :param kwargs: Optional Keyword parameters for the query.
        :return: Result of the Cypher Query as a cursor.
        """
        return self.graph.run(query, **kwargs)

    def get_query_data(self, query, **kwargs):
        """
        This method accepts a Cypher query and returns the result as a list of dictionaries.
        :param query: Cypher Query to run
        :param kwargs: Optional Keyword parameters for the query.
        :return: Result of the Cypher Query as a list of dictionaries.
        """
        return self.get_query(query, **kwargs).data()

    def get_query_df(self, query, **kwargs):
        """
        This method accepts a Cypher query and returns the result as a pandas dataframe.
        :param query: Cypher Query to run
        :param kwargs: Optional Keyword parameters for the query.
        :return: Result of the Cypher Query as a pandas dataframe.
        """
        return self.get_query(query, **kwargs).to_data_frame()