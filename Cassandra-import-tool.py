import logging
import logging.handlers
import time
import sys
import subprocess
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import datetime

file_path = "insert.txt"

def read_file_as_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()  # Reads all lines and removes the newline characters
    return lines

data_list = read_file_as_list(file_path)
# Initialize the Cassandra cluster with pooling
def create_session():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['10.77.0.226'], auth_provider=auth_provider)
    session = cluster.connect('my_keyspace')
    return session

# Insert data using a prepared statement
def insert_data(session, current_dataline):
    insert_stmt = current_dataline
    print(insert_stmt)
    session.execute(insert_stmt)
    print(current_dataline)
    print("Data inserted successfully.")

session = create_session()
for current_dataline in data_list:
    insert_data(session, current_dataline)
