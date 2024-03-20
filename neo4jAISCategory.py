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
            "match (cat:Category) with cat skip $startID limit $endID call { with cat match (u:URL) where u.category = cat.name MERGE (u)-[:HAS_CATEGORY]->(cat) } IN TRANSACTIONS OF 1 rows;"
            )   
            result = session.run(query, startID=startID, endID=endID)

            #result = session.run(query)
            # result = session.write_transaction(self._delete_movies)
            for row in result:
                x=1
            endmillisec = time.time_ns() // 1000000
            print(endmillisec - startmillisec)
            


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
