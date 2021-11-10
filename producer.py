# Author: Aniruddha Gokhale
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 6, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, we use the "top" command and use it as producer of events for
#    Kafka. The consumer can be another Python program that reads and dumps the
#    information into a database OR just keeps displaying the incoming events on the
#    command line consumer (or consumers)
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaProducer  # producer of events
import json
import yfinance as yf
 #hardcoded to the setup I have on my local VMs

# acquire the producer
# (you will need to change this to your bootstrap server's IP addr)
producer = KafkaProducer (bootstrap_servers="3.139.135.215:9092", 
                                          acks=1)  # wait for leader to write to log

stock = ['AAPL','GOOG','MSFT','IBM','ORCL','BAC','C']

for i in range (7):
    data = yf.Ticker(stock[i]).history(period='5y')

    #print(data.to_json())
    #print(type(data))
    j = json.dumps(data.to_json())
    #print(type(j))


           
 # the contents in bytes so we convert it to bytes.
    #
    # Note that here I am not serializing the contents into JSON or anything
    # as such but just taking the output as received and sending it as bytes
    # You will need to modify it to send a JSON structure, say something
    # like <timestamp, contents of top>
    #

    producer.send ("utilizations", value=bytes(j,'ascii'))

    producer.flush ()   # try to empty the sending buffer

    # sleep a second

    time.sleep (1)

# we are done
producer.close ()
    
