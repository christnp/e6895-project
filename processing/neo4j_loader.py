
#!/usr/bin/env python3

from datetime import datetime
import sys
import os
import shutil
import time
import csv
import argparse
import re

import numpy as np

from neo4j import GraphDatabase


GIT_REPO = "https://tbd.com"
class Neo4jUtil:
    """
    This class postprocesses data collected during the personality network
    study, understanding personal value and objectives.
    """


    def __init__(self,n4j_import="file:///demo",node_tail="_Z_raw_vert.csv",
        edge_tail="_Z_raw_edges.csv",n4j_user="neo4j",n4j_pass="admin",
        n4j_uri="localhost",n4j_port="7687", dump=False,collect=False, 
        test=False, pretty=False,verbose=False):
        """
        """

        self.n4j_import = n4j_import # top level path to neo4j import
        self.node_tail = node_tail
        self.edge_tail = edge_tail
        self.dump = dump
        self.collect = collect
        self.test = test
        self.pretty = pretty
        self.verbose = verbose

        n4j_db = "bolt://{uri}:{port}".format(uri=n4j_uri,port=n4j_port)
        print(n4j_db)
        self.driver = GraphDatabase.driver(n4j_db, auth=(n4j_user,n4j_pass))


    def add_friend(self,tx, name, friend_name):
        tx.run("MERGE (a:Person {name: $nn}) "
            "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
            nn=name, friend_name=friend_name)


    def print_friends(self,tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                            "RETURN friend.name ORDER BY friend.name", name=name):
            print(record["friend.name"])


    def dump_db(self,tx):
        tx.run("MATCH (n) DETACH DELETE n")


    def return_n(self,tx):
        for record in tx.run("MATCH (n) RETURN n"):
            print(record)


    def load_nodes(self,tx,csv,ffm):
        query = "LOAD CSV WITH HEADERS FROM "
        query += "\"{csv}\" AS users ".format(csv=csv)
        query += "CREATE(n:{ffm} {{id:toInteger(users.id),label:users.name, " \
                "score:toFloat(users.{ffm}_Z),age:toInteger(users.age), " \
                "sex:users.sex,country:users.country}})".format(ffm=ffm)
        print(query)
        tx.run(query)

    def load_edges(self,tx,csv,ffm):
        query = "LOAD CSV WITH HEADERS FROM "
        query += "\"{csv}\" AS edges ".format(csv=csv)
        query += "MATCH (a:{ffm} {{ id: toInteger(edges.src) }}) " \
                "MATCH (b:{ffm} {{ id: toInteger(edges.dst)   }}) " \
                "CREATE (a)-[r:INTERLOCK{{weight:toFloat(edges.{ffm}_Z)}}]->(b);".format(ffm=ffm) 
        tx.run(query)

    def test_neo4j(self,session):
        session.write_transaction(self.add_friend, "Arthur", "Guinevere")
        session.write_transaction(self.add_friend, "Arthur", "Lancelot")
        session.write_transaction(self.add_friend, "Arthur", "Merlin")
        session.read_transaction(self.print_friends, "Arthur")


    def run(self):
        """MultilayerAnalsis.run()
        Main function that runs the postprocessing
        """

        print("\n----------------------------------------")
        print("Starting \'{}\'".format(self.__class__.__name__))
        print("----------------------------------------\n")

        with self.driver.session() as session:

            # This helps us not duplicate records
            session.write_transaction(self.dump_db)
            if self.dump:
                print("Dumped the database.")
                return
            if self.collect:
                session.read_transaction(self.return_n)
                return
            if self.test:
                self.test_neo4j(session)
                print("Test complete.")
                return
            
            ffms=["OPN","CSN","EXT","AGR","NEU"]
            for ffm in ffms:
                node_path = ffm + self.node_tail
                edge_path = ffm + self.edge_tail
                v_csv_path = "{csv}/{nodes}".format(csv=self.n4j_import,nodes=node_path)
                e_csv_path = "{csv}/{edges}".format(csv=self.n4j_import,edges=edge_path)

                session.write_transaction(self.load_nodes,v_csv_path,ffm)
                session.write_transaction(self.load_edges,e_csv_path,ffm)

            # session.read_transaction(self.return_n)
            # session.write_transaction(dump_db)


        self.driver.close()
     

if __name__ == "__main__":
    prg_desc = 'Batch loads the FFM data from ComplexAnalysis output'
    prg_desc += 'into a neo4j database '
    prg_desc += '{}'.format(GIT_REPO)

    parser = argparse.ArgumentParser(description=prg_desc)

    parser.add_argument('-i','--input',metavar='INPUT_PATH', default="file:///demo",
                        dest='n4j_import',help='Path to data that needs processed.')
    parser.add_argument('-n','--node',metavar='NODE_TAIL',default="_Z_raw_vert.csv",
                        dest='node_tail',help='Last part of node CSV path, to include the .csv.')
    parser.add_argument('-e','--edge',metavar='EDGE_TAIL',default="_Z_raw_edges.csv",
                        dest='edge_tail',help='Last part of edge CSV path, to include the .csv.')
    parser.add_argument('-u','--user',default="neo4j",
                        dest='n4j_user',help='neo4j username.')
    parser.add_argument('-p','--pass',default="admin",
                        dest='n4j_pass',help='neo4j password.')
    parser.add_argument('-r','--uri',default="localhost",
                        dest='n4j_uri',help='neo4j database uri hostname. Default: localhost')
    parser.add_argument('-t','--port',default="7687",
                        dest='n4j_port',help='neo4j database port number.')

    parser.add_argument('-d','--dump',action='store_true',default=False,
                        dest='dump',help='Dump/delete neo4j database.')
    parser.add_argument('-c','--collect',action='store_true',default=False,
                        dest='collect',help='Collect/return all from neo4j database.')
    parser.add_argument('-s','--test',action='store_true',default=False,
                        dest='test',help='Run neo4j test.')
   
    parser.add_argument('-y','--pretty',action='store_true',default=False,
                        dest='pretty',help='Enables pretty output of all files.')
    parser.add_argument('-v','--verbose',action='store_true',default=False,
                        dest='verbose',help='Verbose output.')
    
    args = parser.parse_args()

    load = Neo4jUtil(**vars(args))

    load.run()

          



