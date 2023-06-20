import logging
import os
import psycopg2
import threading
import requests, zipfile, io
import json
from prettytable import PrettyTable
from prettytable import from_db_cursor
from match import persistData


logging.basicConfig(level=logging.DEBUG)
logging.info('Initializing App..')

dataDir = 'external'
sqlDir = 'sql'
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

def readStatements(file ):
    logging.debug('Trying to open sql file %s',file)
    location = sqlDir+'/'+file
    file = open(location)
    file.seek(0)
    sqlFile = file.read()
    if sqlFile == '':
        curDataDir = os.scandir(sqlDir)
        logging.warning('SQL directory: %s', location)

        for entry in curDataDir:
            if entry.is_dir() or entry.is_file():
                logging.warning('SQL directory: %s', entry)
        raise Exception('Couldnt read statements.')
    return sqlFile.split(';')

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
    
    statements = readStatements(os.environ.get('DB_SCHEMA_FILE'))
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
            # if index>100:
            #     break
            fileObj = open(dataDir+'/'+file.name)
            try:
                data = json.load(fileObj)
                persistData(data, conn)
                logging.info('Persisted data file %s',index)
            except Exception as error:
                logging.exception('Error persisting data', exc_info=1)
                
except Exception as error:
    logging.exception("reading json files NOK @ %s", error)

#Get wins and display @ stdout
winsFile = os.environ.get("WINS_BY_TEAM")
winStatements = readStatements(winsFile)

try:
    logging.info("Getting wins per team")
    cursor = conn.cursor()
    cursor.execute(cursor.mogrify(winStatements[0]))
    table = from_db_cursor(cursor)
    print(table)

except Exception as error:
    logging.warning("Error getting wins %s", error)
    

#Get best teams from 2019

bestFile = os.environ.get("WINNINGEST")
logging.info('env var %s', bestFile)
bestStatements = readStatements(bestFile)

try:
    logging.info("Getting best teams from 2019")
    cursor = conn.cursor()
    cursor.execute(cursor.mogrify(bestStatements[0]))
    table = from_db_cursor(cursor)
    print(table)

except Exception as error:
    logging.warning("Error getting wins %s", error)
    


conn.close()
