# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

def process_data(filename):
    print(os.getcwd())
    filepath = os.getcwd() + '/event_data'
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))
        
    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            # extracting each data row one by one and append it 
            for line in csvreader:
                full_data_rows_list.append(line)
                
    # creating a smaller event data csv file called event_datafile_full csv that 
    # will be used to insert data into Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new2.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], \
                             row[7], row[8], row[12], row[13], row[16]))
            
    # check the number of rows in your csv file
    with open(filename, 'r', encoding = 'utf8') as f:
        print("csv file length: ", sum(1 for line in f))
    
def database_design(session):
    
    query = "CREATE TABLE IF NOT EXISTS songs"
    query = query + "(sessionId int, itemInSession int, artist text, song text, length float, \
                PRIMARY KEY (sessionId, itemInSession))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)
    
    query = "CREATE TABLE IF NOT EXISTS song_playlist_session"
    query = query + "(userId int, sessionId int, itemInSession int, artist text, \
            song text, firstName text, lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)
        
    query = "CREATE TABLE IF NOT EXISTS song_listened_by_users"
    query = query + "(song text, userId int, firstName text, lastName text,\
                PRIMARY KEY ((song), userId))"
    try:
        session.execute(query)
    except Exception as e:
        print(e)
    
    print("Tables Created")
    
def inserting_data(session, filename):
    query = "SELECT artist, song, length FROM music_library WHERE sessionId='338' and itemInSession='4'"
    # We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
    # file = 'event_datafile_new2.csv'
    file = filename
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO songs (sessionId, itemInSession, artist, song, length)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"
            
            query2 = "INSERT INTO song_playlist_session (userId, sessionId, itemInSession, \
                                                    artist, song, firstName, lastName)"
            query2 = query2 + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            
            query3 = "INSERT INTO song_listened_by_users (song, userId, firstName, lastName)"
            query3 = query3 + " VALUES (%s, %s, %s, %s)"
            
            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
            session.execute(query2, (int(line[10]), int(line[8]), int(line[3]), \
                                     line[0], line[9], line[1], line[4]))
            session.execute(query3, (line[9], int(line[10]), line[1], line[4]))
    
    print("Data Inserted")
    
def querying(session, query):
    print("Query: ", query)
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
        
    for row in rows:
        print ("Result: ", row)

def drop_tables(session):
    query1 = "DROP TABLE IF EXISTS songs"
    query2 = "DROP TABLE IF EXISTS song_playlist_session"
    query3 = "DROP TABLE IF EXISTS song_listened_by_users"
    try:
        session.execute(query1)
        session.execute(query2)
        session.execute(query3)
    except Exception as e:
        print(e)
    print("Tables Dropped")
        
def main():
    # creating a cluster
    cluster = Cluster()
    session = cluster.connect()
    # Create a Keyspace 
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
    except Exception as e:
        print(e)
    # Set KEYSPACE
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
        
    filename = 'event_datafile_new2.csv'
    process_data(filename)
    database_design(session)
    inserting_data(session, filename)
    
    query = "SELECT artist, song, length FROM songs WHERE sessionId=338 and itemInSession=4"
    querying(session, query)
    query = "SELECT artist, song, firstName, lastName FROM song_playlist_session WHERE userid=10 and sessionId=182"
    querying(session, query)
    query = "SELECT firstName, lastName FROM song_listened_by_users WHERE song='All Hands Against His Own'"
    querying(session, query)
    drop_tables(session)
    
main()
    
