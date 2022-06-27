import sqlite3
import json
import os
from requests_oauthlib import OAuth2Session
import urllib.parse
import csv

def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn

#Get a list of objects and insert any that are missing into ObjectSelection
def getObjectSelection(json):
    #Create ObjectSelection table if it doesn't exist
    with open('sql/ObjectSelection.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.commit()
        conn.close()
    conn = get_db_connection()

    #Insert any new objects into ObjectSelection
    for object in json:
        if object['createable'] == True:
            conn.execute('INSERT INTO ObjectSelection SELECT ?,?,false WHERE ? NOT IN (SELECT ObjectName FROM ObjectSelection)',(object['name'],object['label'],object['name']))
    conn.commit()
    conn.close()

#insert missing fields into FieldMappings give a json from sobjects/[object]/describe
#attempt to map the fields to destination based on DestinationField table
def getFieldMapping(objectName,json):
    with open('sql/FieldMapping.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.commit()
        conn.close()
        conn = get_db_connection()

    #Insert any new objects into ObjectSelection
    for object in json:
        # if object['createable'] == True:
        #     conn.execute('INSERT INTO FieldMapping SELECT ?,?,?,NULL WHERE ? NOT IN (SELECT FieldName FROM FieldMapping)',(object['name'],object['label'],object['name']))
        conn.execute('INSERT INTO FieldMapping(ObjectName, FieldName, FieldLabel, DestinationFieldName) ' +
        'SELECT ?,?,?,(SELECT FieldName FROM DestinationField WHERE ObjectName=? AND FieldName=?) ' +
        ' WHERE ? NOT IN (SELECT FieldName FROM FieldMapping)',(objectName,object['name'],object['label'],objectName, object['name'], object['name']))
    conn.commit()
    conn.close()

    #return a list of FieldMapping records for the object
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ObjectName,FieldLabel,FieldName,DestinationFieldName FROM FieldMapping WHERE ObjectName=?;",(objectName,))
    sourceFields = cursor.fetchall()
    return sourceFields

#get a list of destination fields and insert any that are missing into DestinationField
def getDestinationFields(objectName,json):
    with open('sql/DestinationField.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.commit()
        conn.close()
        conn = get_db_connection()

    #Insert any new objects into DestinationField
    for object in json:
        conn.execute('INSERT INTO DestinationField SELECT ?,?,? WHERE ? NOT IN (SELECT FieldName FROM DestinationField)',(objectName,object['name'],object['label'],object['name']))
    conn.commit()
    conn.close()

    #return a list of DestinationField records for the object
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ObjectName,FieldLabel,FieldName FROM DestinationField WHERE ObjectName=?;",(objectName,))
    destinationFields = cursor.fetchall()
    return destinationFields

#insert fields into SourceMetadata table based on json
def getMetadataFromSource(json,object_name):

    #Drop and recreate Metadata table
    with open('sql/SourceMetadata.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.execute('DELETE FROM SourceMetadata WHERE Object=?',(object_name,))
        conn.commit()
        conn.close()
    conn = get_db_connection()
    for field in json:
        conn.execute('INSERT INTO SourceMetadata (Object, Field) VALUES (?,?)',(object_name, field['name']))
    conn.commit()
    conn.close()
    return 1 

#generate a CREATE TABLE statement based on SourceMetadata table
def createSchemaFromSource(object_name):
    #Create table name
    table_name = 'Source'+object_name


    #Delete the file if it exists
    if os.path.isfile('sql/'+table_name+'.sql'):
        os.remove('sql/'+table_name+'.sql')


    create_statement = 'DROP TABLE IF EXISTS '+table_name+'; \nCREATE TABLE '+table_name+'(\n'

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Field FROM SourceMetadata WHERE Object='" + object_name+"'")
    fields = cursor.fetchall()
    first = True
    f = open('sql/'+table_name+'.sql',"w+")
    for field in fields:
        if first:
            create_statement += field[0] + ' TEXT\n'
            first = False
        else:
            create_statement += ',' + field[0] + ' TEXT\n'
    create_statement += ')'
    f.write(create_statement)
    f.close()
    #run the CREATE TABLE statement
    with open('sql/'+table_name+'.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.commit()
        conn.close()    

#return a url encoded SELECT statement for a given object based on SourceMetadata
#TODO: maybe in the future this should be pulling from FieldMapping where DestinationFieldName IS NOT NULL
def getEncodedQuery(object_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Field FROM SourceMetadata WHERE Object='" + object_name + "'")
    fields = cursor.fetchall()
    query = 'SELECT '
    first = True
    for field in fields:
        if first:
            query += field[0]
            first = False
        else:
            query += ' ,' + field[0]
    query += ' FROM ' + object_name
    encodedUrl = urllib.parse.quote(query)
    return encodedUrl
        
#insert records into a Source[Object] SQL table given json
def insertRecordsFromJson(json,object_name):
    #DB connection
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Field FROM SourceMetadata WHERE Object='"+object_name+"'")
    rows = cursor.fetchall()

    table_name= 'Source'+object_name

    #loop through each record and insert into sql 
    for record in json:
        insertStmt = "INSERT INTO " + table_name
        fieldsList = "("
        valuesList = "("

        #don't need attributes
        del record['attributes']

        first = True
        for field in record:
            value = record[field]
            if first:
                fieldsList += field
                valuesList += "'" + str(value).replace("'","''") + "'" #this might be a dictionary
                first = False
            else:
                fieldsList += " ," + field
                valuesList += " ," + "'" + str(value).replace("'","''") + "'"
                
        fieldsList += ")"
        valuesList += ")"
        insertStmt += " " + fieldsList + " VALUES " + valuesList + "; "
        cursor.execute(insertStmt)
    conn.commit()
    conn.close()

    return len(json) #TODO: put this in javascript alert for how many records

    #how many question marks
    
#Create and run a script for a SQL insert table given the name of an object
#based on 
def createInsertTable(object_name):
    insert_table_name = object_name + '_insert'
    source_table_name = 'Source' + object_name

    #create the script
    #Delete the file if it exists
    if os.path.isfile('sql/'+insert_table_name+'.sql'):
        os.remove('sql/'+insert_table_name+'.sql')

    insert_table_statement = 'DROP TABLE IF EXISTS '+insert_table_name+'; \nCREATE TABLE ' + insert_table_name + ' as SELECT\n'

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT FieldName,DestinationFieldName FROM FieldMapping WHERE ObjectName='" + object_name+"' AND DestinationFieldName IS NOT NULL")
    fields = cursor.fetchall()
    first = True
    f = open('sql/'+insert_table_name+'.sql',"w+")
    for row in fields:
        if first:
            if row[0] == row[1]:
                insert_table_statement += row[0] + '\n'
            else:
                insert_table_statement += row[0] + ' as ' + row[1] + '\n'
            first = False
        else:
            if row[0] == row[1]:
                insert_table_statement += ',' + row[0] + '\n'
            else:
                insert_table_statement += ',' + row[0] + ' as ' + row[1] + '\n'    
    insert_table_statement += 'FROM ' + source_table_name
    f.write(insert_table_statement)
    f.close()


    #run the script
    with open('sql/'+insert_table_name+'.sql') as f:
        conn = get_db_connection()
        conn.executescript(f.read())
        conn.commit()
        conn.close()    

#Create a csv export from sql table given SQL table name
def exportToCSV(table_name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM ' + table_name)
    #rows = cur.fetchall()

    #remove file if it's already there
    if os.path.isfile('./'+table_name+'.csv'):
        os.remove('./'+table_name+'.csv')


    with open(table_name+'.csv','w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter="|")
        csv_writer.writerow([i[0] for i in cur.description])
        csv_writer.writerows(cur)
        
#create a batch job 


