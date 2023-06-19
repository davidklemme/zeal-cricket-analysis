import logging
import os
import psycopg2
import threading
import requests, zipfile, io
import json
from match import persistData


logging.basicConfig(level=logging.DEBUG)
logging.info('Initializing App..')

dataDir = 'external'
url = os.environ["CRICKET_DATA_URL"]
database = os.environ["POSTGRES_DB"]
dbUser = os.environ["POSTGRES_USER"]
dbPass = os.environ["POSTGRES_PASSWORD"]

logging.info("Trying database %s", database)
result_available = threading.Event()

conn = psycopg2.connect(dbname=database, user=dbUser, host='db', password=dbPass, connect_timeout=1)
cursor = conn.cursor()
cursor.execute("DROP SCHEMA public CASCADE;")
cursor.execute("CREATE SCHEMA public;")

def getExternalData() : 
    numberOfFiles = 0    
    response = requests.get(url, stream='true')
    content = zipfile.ZipFile(io.BytesIO(response.content))
    content.extractall(dataDir)

    for files in os.listdir(dataDir):
        numberOfFiles += len(files)    

    logging.debug('Downloaded and extracted %s files', numberOfFiles)
    
    result_available.set()

def createSchema(cursor) :
    schemaFile = open('sql/db_schema.sql')
    sqlFile = schemaFile.read()
    statements = sqlFile.split(';')
    for statement in statements:
        if statement == '': 
            break
        cursor.execute(statement)
    
    conn.commit()

try:
    logging.info("Downloading data from %s", url)
    thread = threading.Thread(target=getExternalData)
    thread.start()

    result_available.wait()
    cursor = conn.cursor()
    createSchema(cursor)
    
    
    logging.info("Database connection ok @ %s", conn)
except Exception as error:
    logging.error("Database connection NOK @ %s", error)
    os._exit(os.EX_NOHOST)



try:    
    logging.info('Persisting data ..')
    files = os.scandir(dataDir)
    cursor = conn.cursor()
    for index,file in enumerate(files):
            fileObj = open(dataDir+'/'+file.name)
            try:
                data = json.load(fileObj)
                persistData(data, conn)
                logging.info('Persisted data file %s',index)
            except Exception as error:
                logging.exception('Error persisting data', exc_info=1)
                os._exit(os.EX_NOHOST)
    

except Exception as error:
    logging.exception("reading json files NOK @ %s", error)
    os._exit(os.EX_NOINPUT)

logging.info("Exiting app.") 
conn.close()
