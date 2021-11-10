#
#
# Author: Aniruddha Gokhale
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 6, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, demonstrate Kafka streaming API to build a consumer.
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaConsumer  # consumer of events
import uuid
import couchdb
import json
import pycouchdb
import ast

# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

couchDBUser = "admin"
couchDBPwd = "password"
couchDBDB = "stock_data"

couch = pycouchdb.Server( f"http://{couchDBUser}:{couchDBPwd}@3.139.135.215:5984")
db = couch.create( couchDBDB )

# acquire the consumer
# (you will need to change this to your bootstrap server's IP addr)
consumer = KafkaConsumer (bootstrap_servers="3.139.135.215:9092,172.31.23.28:9092")

# subscribe to topic
consumer.subscribe (topics=["utilizations"])

# we keep reading and printing
try:    
    for msg in consumer:
        # what we get is a record. From this record, we are interested in printing
        # the contents of the value field. We are sure that we get only the
        # utilizations topic because that is the only topic we subscribed to.
        # Otherwise we will need to demultiplex the incoming data according to the
        # topic coming in.
        #
        # convert the value field into string (ASCII)
        #
        # Note that I am not showing code to obtain the incoming data as JSON
        # nor am I showing any code to connect to a backend database sink to
        # dump the incoming data. You will have to do that for the assignment.

        #id = uuid.uuid1(
        #print(msg)
        print( f"--->>> Adding to DB {id}" )
        
        data=json.loads(str(msg.value,'ascii'))
       
        rec=ast.literal_eval(data)
       # print(type(rec))
        doc = db.save(rec)
     
        #print(doc) 

except KeyboardInterrupt:
    print(f"\n\nAdded to DB\n\n")
    for id in db:
        print( f"id={id}: {db[id]}" )
        ### db.delete(db[id])

#
# Delete our database, since we do not care about data for the long term
#
couch.delete( couchDBDB )

# we are done. As such, we are not going to get here as the above loop
# is a forever loop.
consumer.close ()
