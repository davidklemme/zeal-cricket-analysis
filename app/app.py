import logging
import time
import os
import psycopg2
import psycopg2.extras as extras
import threading
import requests, zipfile, io
import json
from prettytable import PrettyTable
from prettytable import from_db_cursor
from match import persistData


logging.basicConfig(
                    # filename='app.log',
                    # filemode='a',
                    # format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    # datefmt='%H:%M:%S',
                    level=logging.DEBUG)
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
    '''
    read the statements into memory from file to execute against the DB via psycopg2
    '''
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
    '''
    Get External data, as defined by env var /url/ and the set data directory
    '''
    numberOfFiles = 0    
    response = requests.get(url, stream='true')
    content = zipfile.ZipFile(io.BytesIO(response.content))
    content.extractall(dataDir)

    for files in os.listdir(dataDir):
        numberOfFiles += len(files)    

    logging.debug('Downloaded and extracted %s files', numberOfFiles)
    
    result_available.set()

def createSchema(cursor) :
    '''
    Creating tables in the DB, based on schema file defined in sql folder. File is specified as a env var.
    Function will iterate over all statements it finds in the file.
    '''
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
    '''need to wait for download to be completed before we can read and extract the data'''
    result_available.wait()
    cursor = conn.cursor()
    createSchema(cursor)
    
    
    logging.info("Database connection ok @ %s", conn)
except Exception as error:
    logging.error("Database connection NOK @ %s", error)
    os._exit(os.EX_NOHOST)



try:    
    '''
    Look into the defined data directory and iterate over all files.
    Files are assumed to be valid JSON format. If they are not, we will log the exception and continue. 
    This would not be sufficient in certain prod environments, but I'll skip the handler for this exercise.
    '''
    logging.info('Persisting data ..')
    start = time.time()
    logging.debug('Start time: %s',start)
    files = os.scandir(dataDir)
    cursor = conn.cursor()
    for index,file in enumerate(files):
    
            fileObj = open(dataDir+'/'+file.name)
            try:
                '''
                Current approach loading single files to persist is non-performant (intake takes  roughly 35 mins as is), but I unfortunately have no more time to rewrite.
                Better approach would be to fully utilizet the extras batch approach, which can also take in an array as the values argument.
                Should have from the start build a data object, and the persisted the parts of the object.
                '''
                data = json.load(fileObj)
            
                persistData(data, conn, extras)
                if index % 10 == 0:
                    logging.info('Persisted data file %s',index)
            except Exception as error:
                logging.exception('Error persisting data', exc_info=1)
    
    print(matchesObj)

    end = time.time()
    logging.debug('Total time elapsed %s', end-start)      
except Exception as error:
    logging.exception("reading json files NOK @ %s", error)


logging.info('Done loading data.')
'''
Define array for the results to be displayed.
Iterate over array and display data directly via stdout
'''
selectsArray = ['WINS_BY_TEAM','WINNINGEST','BATTING']

selectsDescription = [
    'The win records (percentage win and total wins) for each team by year and gender, excluding ties, matches with no result, and matches decided by the DLS method in the event that, for whatever reason, the planned innings cant be completed',
    'Which male and female teams had the highest win percentages in 2019? Tie breaker is the total number of wins',
    'Which players had the highest strike rate as batsmen in 2019?'
    ]

for i,it in enumerate(selectsArray):
    file= os.environ.get(it)
    statements = readStatements(file)

    try:
        logging.info("Getting %s", it)
        cursor = conn.cursor()
        cursor.execute(cursor.mogrify(statements[0]))
        table = from_db_cursor(cursor)
        print('############################')
        print(selectsDescription[i])
        print('##############')
        print(table)
        print('############################')

    except Exception as error:
        logging.warning("Error getting wins %s", error)
        


conn.close()
