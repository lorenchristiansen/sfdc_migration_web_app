import sqlite3
from flask import Flask, flash, redirect, render_template, request, url_for, session
from flask_session import Session
from flask.json import jsonify
from requests_oauthlib import OAuth2Session
import requests
from DatabaseHelper import get_db_connection, getDestinationFields, insertRecordsFromJson, getMetadataFromSource, createSchemaFromSource, getEncodedQuery, getObjectSelection, getFieldMapping, getDestinationFields, createInsertTable, exportToCSV
import os
import json

app = Flask(__name__)
app.config['SECRET_KEY'] = '5618135d4f3a5sd13as5d43as5df13as5dg13as5df43ad5sg43a5sd4f31a2sd' #random
app.config["SESSION_PERMANENT"] = False
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1' #This is only for testing to disable requirement for SSL

# http://localhost:5000

#Source
#client_id = 3MVG9p1Q1BCe9GmA_Y1H7w7sNcaNr9SVg19PVEPgiyORg.6r249T.FpzUlOKf2t2kmDUtiXai7rJmyjxucvm7
#client_secret = 26ACF8CF17CE5957A6AB02DEB098A368A483F86A30EA64769D813F8C47A150E9


#Destination
#client_id = 3MVG9eQyYZ1h89HeQA4CF9MzAVatUp83n7wpZnLjE.0Eunu0EZCMZ1_ROdatzdGfY1nzcbRYhQLXGSfzkXbaa
#client_secret = 544686B00CAB2AC00FDA74437AEC9C4C1D6A89DD06263FAA78CCFB0784D966B4



@app.route('/')
def index():
    return render_template('index.html')


###################################### SOURCE LOGIN ############################################

request_url = 'https://login.salesforce.com/services/oauth2/authorize'
token_url = 'https://login.salesforce.com/services/oauth2/token'
source_instance_url = 'https://saasconsultinggroup-2f-dev-ed.my.salesforce.com'
source_redirect_uri_accesstoken = 'http://localhost:5000/source_access_token'

dest_request_url = 'https://CS45.salesforce.com/services/oauth2/authorize'
dest_token_url = 'https://CS45.salesforce.com/services/oauth2/token'
dest_instance_url = 'https://power-velocity-6149-dev-ed.cs45.my.salesforce.com/'
dest_redirect_uri_accesstoken = 'http://localhost:5000/destination_access_token'

redirect_uri_success = 'http://localhost:5000/success'


@app.route('/source_login',methods = ['POST', 'GET'])
def source_login():
    if request.method == 'POST':
        session['source_client_id'] = request.form['client_id']
        session['source_client_secret'] = request.form['client_secret']
        return redirect(url_for('.login_to_source'))
        #return session['source_client_id']
    return render_template('source_login.html')

#OAuth for source
@app.route('/login_to_source')
def login_to_source():
##First, get the user to login
    oAuthSession = OAuth2Session(client_id=session['source_client_id'],redirect_uri=source_redirect_uri_accesstoken)
    authorization_url, state = oAuthSession.authorization_url(request_url)
    #session['oauth_state'] = state #Might need later, not working
    return redirect(authorization_url)


@app.route('/source_access_token')
def source_access_token():
    oAuthSession = OAuth2Session(client_id=session['source_client_id'],redirect_uri=redirect_uri_success)
    token = oAuthSession.fetch_token(token_url,client_secret=session['source_client_secret'],authorization_response=request.url)
    session['source_token'] = token
    return redirect(url_for('.source_login'))


################################################ DEST LOGIN #########################################################
@app.route('/destination_login',methods = ['POST', 'GET'])
def destination_login():
    if request.method == 'POST':
        session['destination_client_id'] = request.form['client_id']
        session['destination_client_secret'] = request.form['client_secret']
        return redirect(url_for('.login_to_destination'))
        #return session['destination_client_id']
    return render_template('destination_login.html')

#OAuth for destination
@app.route('/login_to_destination')
def login_to_destination():
##First, get the user to login
    # oAuthSession = OAuth2Session(client_id=session['destination_client_id'],redirect_uri=redirect_uri_accesstoken)
    oAuthSession = OAuth2Session(client_id=session['destination_client_id'],redirect_uri=dest_redirect_uri_accesstoken)
    authorization_url, state = oAuthSession.authorization_url(dest_request_url)
    #session['oauth_state'] = state #Might need later, not working  
    return redirect(authorization_url)


@app.route('/destination_access_token')
def destination_access_token():
    oAuthSession = OAuth2Session(client_id=session['destination_client_id'],redirect_uri=redirect_uri_success)
    token = oAuthSession.fetch_token(dest_token_url,client_secret=session['destination_client_secret'],authorization_response=request.url)
    session['destination_token'] = token
    return redirect(url_for('.destination_login'))


################################################ OBJECT MAPPING #################################################

@app.route('/object_selection')
def object_selection():
    #Get any new objects
    sfdc = OAuth2Session(client_id=session['source_client_id'], token=session['source_token'])
    metadataRecords = sfdc.get(source_instance_url + '/services/data/v52.0/sobjects/').json()
    getObjectSelection(metadataRecords['sobjects'])

    #pull a list of available objects from ObjectSelection
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ObjectLabel,ObjectName,Migrate FROM ObjectSelection;")
    objects = cursor.fetchall()
    
    return render_template('object_selection.html', objects=objects)

@app.route('/update_object_selection', methods=['POST'])
def update_object_selection():
    object_name = request.form['object_name']
    migrate = 0
    if request.form['migrate'] == "true":
        migrate = 1
    conn = get_db_connection()
    conn.execute('UPDATE ObjectSelection SET Migrate=? WHERE ObjectName=?',(migrate,object_name))
    conn.commit()
    conn.close()
    return "success"

################################################ FIELD MAPPING #################################################


@app.route('/field_mapping')
def field_mapping_list():
    #Check for the objects in ObjectSelection that have Migrate=1
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ObjectLabel,ObjectName FROM ObjectSelection WHERE Migrate=1 ORDER BY ObjectLabel;")
    objects = cursor.fetchall()


    #Display table of available objects
    return render_template('field_mapping.html', objects=objects)

    #each object name should link to a field mapping page


@app.route('/field_mapping_record/<objectName>')
def field_mapping_record(objectName):
    #get destination fields for the object
    sfdc_dest = OAuth2Session(client_id=session['destination_client_id'], token=session['destination_token'])
    metadataRecords = sfdc_dest.get(dest_instance_url + '/services/data/v52.0/sobjects/'+ objectName + '/describe').json()
    destinationFields = getDestinationFields(objectName,metadataRecords['fields']) 


    #get any new fields for the specific object to insert into FieldMapping SQL table
    sfdc = OAuth2Session(client_id=session['source_client_id'], token=session['source_token'])
    metadataRecords = sfdc.get(source_instance_url + '/services/data/v52.0/sobjects/'+ objectName + '/describe').json()
    sourceFields = getFieldMapping(objectName,metadataRecords['fields']) #this will take a first pass at mapping

    return render_template('field_mapping_record.html',sourceFields=sourceFields, destinationFields = destinationFields)


@app.route('/update_field_mapping', methods=['POST'])
def update_field_mapping():
    destination_field = request.form['destination_field']
    field_name = request.form['field_name']
    object_name = request.form['object_name']
    conn = get_db_connection()
    conn.execute('UPDATE FieldMapping SET DestinationFieldName=? WHERE ObjectName=? AND FieldName=?',(destination_field,object_name,field_name))
    conn.commit()
    conn.close()
    return "success"



################################################ MIGRATION #################################################

@app.route('/migration_console')
def migration_console():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ObjectLabel,ObjectName FROM ObjectSelection WHERE Migrate=1 ORDER BY ObjectLabel;")
    objects = cursor.fetchall()

    return render_template('migration_console.html', objects=objects)


#pull records from source
@app.route('/pullFromSource', methods = ['POST'])
def pullFromSource():
    object_name = request.form['objectName']

    # #get the metadata
    sfdc = OAuth2Session(client_id=session['source_client_id'], token=session['source_token'])
    metadataRecords = sfdc.get(source_instance_url + '/services/data/v52.0/sobjects/' + object_name + '/describe').json()
    getMetadataFromSource(metadataRecords['fields'],object_name)
    createSchemaFromSource(object_name)

    # #insert records into sql
    encodedQuery = getEncodedQuery(object_name)
    objectRecords = sfdc.get(source_instance_url + '/services/data/v53.0/query/?q=' + encodedQuery).json()
    recordCount = insertRecordsFromJson(objectRecords['records'],object_name)
    flash('Records pulled from source: '+str(recordCount))
    return str(recordCount)


#push records to destination
@app.route('/pushToDestination', methods = ['POST'])
def pushToDestination():
    
    #create and run insert table script 
    object_name = request.form['objectName']
    createInsertTable(object_name)

    #export insert table to csv
    exportToCSV(object_name+'_insert')

    #create batch job in salesforce
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "object": object_name
        ,"contentType": "CSV"
        ,"operation": "insert"
        ,"columnDelimiter": "PIPE"
    }
    sfdc = OAuth2Session(client_id=session['destination_client_id'], token=session['destination_token'])
    response = sfdc.post(dest_instance_url + 'services/data/v50.0/jobs/ingest' ,data = json.dumps(data) ,headers = headers).json()
    # response_json = json.loads(response.text)
    job_id = response['id']

    #send data
    with open(object_name + '_insert.csv') as insert_file:
        response = sfdc.put(dest_instance_url + 'services/data/v50.0/jobs/ingest/' + job_id + '/batches' ,data = insert_file).json()

    #check status? 
    return "success" 




