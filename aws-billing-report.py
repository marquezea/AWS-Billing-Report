import boto3
import os
import gzip
import json
import sqlite3
import datetime
from pathlib import Path
from decimal import Decimal
from tabulate import tabulate
from termgraph import termgraph as tg

# # CONSTANTS
# BILLING_REPORT_BUCKET = 'amarquezelogs'
# BILLING_REPORT_BUCKET_PATH = 'costreport/AMMCostReport/20210201-20210301/20210303T011135Z/'
# OUTPUT_DATABASE_FILENAME = 'awsbilling.db'
# PROFILE_NAME='pythonAutomation'

# CONSTANTS
BILLING_REPORT_BUCKET = 'backup-chipr-denis'
BILLING_REPORT_BUCKET_PATH = 'report/billing_report/20210201-20210301/20210303T110148Z/'
OUTPUT_DATABASE_FILENAME = 'awsbilling.db'
PROFILE_NAME='denischipr'

# DELETE A FILE
def deleteFile(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print("The file does not exist")

# UNZIP GZ FILE
def unzipFile(gzFilename):
    gzFile = gzip.open(gzFilename, 'rb')
    file_content = gzFile.read()
    gzFile.close()

    unzipedFile = open(Path(gzFilename).stem, "wb")
    unzipedFile.write(file_content)
    unzipedFile.close()

    deleteFile(gzFilename)

    return Path(gzFilename).stem

# CREATE SQLITE DB
def createMemoryDatabase(extractColumnList, fileStructureFromManifest):
    memDb = sqlite3.connect(':memory:')
    dbCursor = memDb.cursor()
    sql = 'BEGIN TRANSACTION;'
    dbCursor.execute(sql)
    sql = 'CREATE TABLE LINE_ITEMS (\n'
    for index, column in enumerate(extractColumnList):
        dbColumnName = column.replace('/', '_')
        if (fileStructureFromManifest[column] == 'String'):
           dbColumnDataType = 'TEXT'
        if (fileStructureFromManifest[column] == 'BigDecimal'):
           dbColumnDataType = 'NUMBER'
        if (fileStructureFromManifest[column] == 'DateTime'):
           dbColumnDataType = 'TEXT'
        if ((index+1) == len(extractColumnList)):
            sql = sql + dbColumnName + ' ' + dbColumnDataType + ' NOT NULL\n'
        else:
            sql = sql + dbColumnName + ' ' + dbColumnDataType + ' NOT NULL,\n'
    sql = sql + ');'
    dbCursor.execute(sql)
    return memDb

# FLUSH SQLLITE DB IN MEMORY TO DISK
def flushMemoryDatabaseToDisk(memoryDb):
    if os.path.isfile(OUTPUT_DATABASE_FILENAME):
        os.remove(OUTPUT_DATABASE_FILENAME)
    fileDB = sqlite3.connect(OUTPUT_DATABASE_FILENAME)
    with fileDB:
        for line in memoryDb.iterdump():
            if line not in ('BEGIN;', 'COMMIT;'): # let python handle the transactions
                fileDB.execute(line)
    fileDB.commit()

# INSERT RECORD ON DATABASE
def insertRecord(memoryDB, extractColumnList, columnValues, columnDatatypes, fileStructureFromManifest):
    sql = 'INSERT INTO LINE_ITEMS (\n'
    for index, column in enumerate(extractColumnList):
        if ((index+1) == len(extractColumnList)):
            sql = sql + column.replace('/','_') + ') VALUES (\n'
        else:
            sql = sql + column.replace('/','_') + ',\n' 
    for index, columnValue in enumerate(columnValues):
        if (fileStructureFromManifest[extractColumnList[index]] == 'String'):
            convertedValue = '"' + columnValue + '"' 
        if (fileStructureFromManifest[extractColumnList[index]] == 'DateTime'):
            convertedValue = '"' + columnValue.strftime('%Y-%m-%d') + '"' 
        if (fileStructureFromManifest[extractColumnList[index]] == 'BigDecimal'):
            convertedValue = str(columnValue)
        if ((index+1) == len(columnValues)):
            sql = sql + convertedValue + ');\n'
        else:
            sql = sql + convertedValue + ',\n' 
    dbCursor = memoryDB.cursor()
    dbCursor.execute(sql)
    dbCursor.execute('COMMIT;')

# QUERY DATABASE
def queryDatabase(memoryDB, query,title):
    dbCursor = memoryDB.cursor()
    dbCursor.execute(query)
    result = dbCursor.fetchall()
    print('\n' + title)
    print("=" * len(title))
    print(tabulate(result, ['Service','Sum'], tablefmt='psql', numalign='right', stralign='left'))


# FETCH CSV FILE STRUCTURE FROM JSON MANIFEST
def fetchFileStructureFromManifest(filename):
    jsonManifestFile = open(filename)
    jsonManifest = json.loads(jsonManifestFile.read())
    jsonManifestFile.close()

    fileStructure = {}
    for column in jsonManifest['columns']:
        columnName = column['category'] + '/' + column['name']
        columnDataType = column['type']
        fileStructure[columnName] = columnDataType
    return fileStructure

# IMPORT CSV FILE INTO MEMORY DATABASE
def importCsvToDatabase(csvFilename, memoryDB, extractColumnList, fileStructureFromManifest):

    with open(csvFilename) as f:
        #print(extractColumnList)

        # read header line with the column names
        columnHeader = f.readline().split(',')
        # print the index of each extract column
        columnIndexes = []
        for field in extractColumnList:
            columnIndex = columnHeader.index(field)
            columnIndexes.append(columnIndex)
        #print(columnIndexes)
        # print the datatype of each extract column
        columnDatatypes = []
        for field in extractColumnList:
            columnDatatype = fileStructureFromManifest[field]
            columnDatatypes.append(columnDatatype)
        #print(columnDatatypes)
        # iterate over file and get each field value
        for line in f:
            record = line.split(',')
            columnValues = []
            for field in extractColumnList:
                columnIndex = columnHeader.index(field)
                columnValue = record[columnIndex]
                if (fileStructureFromManifest[field] == 'String'):
                    columnValues.append(record[columnIndex])
                if (fileStructureFromManifest[field] == 'BigDecimal'):
                    columnValues.append(Decimal(record[columnIndex]))
                if (fileStructureFromManifest[field] == 'DateTime'):
                    #columnValues.append(record[columnIndex])
                    columnValues.append(datetime.datetime.strptime(record[columnIndex],'%Y-%m-%dT%H:%M:%SZ'))
            # if (columnValues[0] == 'Usage'):
                #print(columnValues)
            insertRecord(memoryDB, extractColumnList, columnValues, columnDatatypes, fileStructureFromManifest)

# LIST CONTENT OF DIRECTORY AND DOWNLOAD
def downloadFilesFromBucket(bucket_name, bucket_path):
    bucketContents = s3.list_objects(Bucket=bucket_name,Prefix=bucket_path)['Contents']
    downloadFiles = []
    for item in bucketContents:
        filenameWithPath = item['Key']
        filename = Path(filenameWithPath).name
        #print(filenameWithPath, filename)
        s3.download_file(BILLING_REPORT_BUCKET, filenameWithPath, filename)
        if (filename[-3:] == '.gz'):
            downloadFiles.append(unzipFile(filename))
        if (filename[-5:] == '.json'):
            downloadFiles.append(filename)
            fetchFileStructureFromManifest(filename)

    return downloadFiles

# INITIALIZE BOTO3
# choose profile to be used 
boto3.setup_default_session(profile_name=PROFILE_NAME)

# GLOBAL VARIABLES
extractColumnList = ['lineItem/LineItemType', 'lineItem/ProductCode', 'lineItem/UsageStartDate', 'lineItem/UsageEndDate', 'lineItem/UsageType', 'lineItem/Operation', 'lineItem/BlendedCost', 'lineItem/UnblendedCost']


# MAIN FLOW
s3 = boto3.client('s3') 
downloadedFiles = downloadFilesFromBucket(BILLING_REPORT_BUCKET, BILLING_REPORT_BUCKET_PATH)
#downloadedFiles = ['AMMCostReport-00001.csv', 'AMMCostReport-Manifest.json']
print(downloadedFiles)
fileStructureFromManifest = fetchFileStructureFromManifest(downloadedFiles[1])

memoryDb = createMemoryDatabase(extractColumnList, fileStructureFromManifest)
importCsvToDatabase(downloadedFiles[0], memoryDb, extractColumnList, fileStructureFromManifest)
queryDatabase(memoryDb, 'SELECT min(lineItem_UsageStartDate) as USAGE_START, max(lineItem_UsageEndDate) as USAGE_END, round(sum(lineItem_BlendedCost),2) as TOTAL  FROM LINE_ITEMS', 'PERIODO')
queryDatabase(memoryDb, 'SELECT lineItem_ProductCode, round(SUM(lineItem_BlendedCost),2) FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_ProductCode', 'SERVICOS')
queryDatabase(memoryDb, 'SELECT lineItem_LineItemType, round(SUM(lineItem_BlendedCost),2) FROM LINE_ITEMS GROUP BY lineItem_LineItemType', 'IMPOSTOS')
flushMemoryDatabaseToDisk(memoryDb)
