import logging
import sys
import csv
import random
import time

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def query_shipping_sites(self,startID,endID):
        cCode = "\u0014";
        newLine = "\n"
        currDB = "iasdb"

        with self.driver.session(database=currDB) as session:
            # Write transactions allow the driver to handle retries and transient errors
            startmillisec = time.time_ns() // 1000000
            query = (
            "match (s:Segment) with s skip $startID limit $endID call { with s match (u:URL) where u.segment = s.segment_id MERGE (u)-[:CONTAINS]->(s) } IN TRANSACTIONS OF 1 rows;"
            )   
            result = session.run(query, startID=startID, endID=endID)

            #result = session.run(query)
            # result = session.write_transaction(self._delete_movies)
            for row in result:
                x=1
            endmillisec = time.time_ns() // 1000000
            print(endmillisec - startmillisec)
            

    def create_friendship(self, person1_name, person2_name, knows_from):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name, knows_from)
            for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}"
                      .format(
                          p1=row['p1'],
                          p2=row['p2'],
                          knows_from=row["knows_from"]))

    def remove_movies(self):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            query = (
            "MATCH (p:Person)-[r:ACTED_IN]->(m) Call {WITH r DELETE r} IN TRANSACTIONS OF 10 ROWS; "
        )
            result = session.run(query)
            # result = session.write_transaction(self._delete_movies)
            for row in result:
                print("Deleted Movies")

    @staticmethod
    def _delete_movies(tx):
           # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "MATCH (p:Person)-[r:ACTED_IN]->(m) Call {WITH r DELETE r} IN TRANSACTIONS OF 10 ROWS; "
        )
        result = tx.run(query)
        try:
            return 1
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name, knows_from):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[k:KNOWS { from: $knows_from }]->(p2) "
            "RETURN p1, p2, k"
        )
        result = tx.run(query, person1_name=person1_name,
                        person2_name=person2_name, knows_from=knows_from)
        try:
            return [{
                        "p1": row["p1"]["name"],
                        "p2": row["p2"]["name"],
                        "knows_from": row["k"]["from"]
                    }
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session(database='fmr', impersonated_user="user2") as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    def loadShippingSites(self):
        siteList = []
        with open('/Users/davidfauth/tmp/reports/shippingsites.csv') as fd:
            reader = csv.reader(fd)
            for row in reader:
                siteList.append(row)
        return siteList

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (n:Portfolio {name:'Portfolio_2'})-[]->(a:Asset) "
            "RETURN a.symbol AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    bolt_url = "neo4j://13.58.250.90:7687"
    user = "neo4j"
    password = "Amish_2020_Tesla"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    # open file to write out
    startSegment = int(sys.argv[1])
    endSegment = int(sys.argv[2])
    app.query_shipping_sites(startSegment,endSegment)

    app.close()
