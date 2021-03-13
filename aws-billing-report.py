import boto3
import os
import gzip
import json
import sqlite3
import datetime
import sys
from pathlib import Path
from decimal import Decimal
from tabulate import tabulate
from termgraph import termgraph as tg

# # CONSTANTS
# CACHE_PATH = './cache/'
# BILLING_REPORT_BUCKET = 'amarquezelogs'
# BILLING_REPORT_BUCKET_PATH = 'costreport/AMMCostReport/20210201-20210301/20210303T011135Z/'
# PROFILE_NAME='pythonAutomation'

# CONSTANTS
CACHE_PATH = './cache/'
# BILLING_REPORT_BUCKET = 'backup-chipr-denis'
# BILLING_REPORT_BUCKET_PATH = 'report/billing_report/20210301-20210401/20210313T184621Z/'
# PROFILE_NAME='denischipr'

# GET COMMAND LINE ARGUMENTS
def commandLineVerification():
    commandLineResult = {}
    isOk = True
    commandLineArguments = []
    for i, arg in enumerate(sys.argv):
        if (arg.lower() == '--bucket') or (arg.lower() == '--profile') or (arg.lower() == '--billing-report-path'):
            commandLineArguments.append(arg.lower())
        else:
            commandLineArguments.append(arg)
        if (arg.lower()[0:2] == '--'):
            if (arg.lower() != '--bucket') and (arg.lower() != '--profile') and (arg.lower() != '--billing-report-path'):
                isOk = False
                print('error: unknown parameter ' + arg.lower() + '\n')
    if (isOk):
        try:
            bucketName = commandLineArguments[commandLineArguments.index('--bucket')+1]
            commandLineResult['--bucket'] = bucketName

            billingReportPath = commandLineArguments[commandLineArguments.index('--billing-report-path')+1]
            commandLineResult['--billing-report-path'] = billingReportPath

            try:    
                profile = commandLineArguments[commandLineArguments.index('--profile')+1]
            except:
                profile = 'default'
            commandLineResult['--profile'] = profile
        except:
            print('usage: python aws-billing-report.py [--profile <aws-cli-profile-name>] --bucket <bucket-name> --path <path-to-billing-report>')
    commandLineResult['status'] = isOk
    return commandLineResult

# DELETE A FILE
def deleteFile(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print("The file does not exist")

# UNZIP GZ FILE
def unzipFile(cachePath, gzFilename):
    gzFile = gzip.open(cachePath + gzFilename, 'rb')
    file_content = gzFile.read()
    gzFile.close()

    unzipedFile = open(cachePath + gzFilename[:-3], "wb")
    unzipedFile.write(file_content)
    unzipedFile.close()

    deleteFile(cachePath + gzFilename)

    return gzFilename[:-3]

# CREATE SQLITE DB
def createMemoryDatabase(extractColumnList, fileManifest):
    memDb = sqlite3.connect(':memory:')
    memDb.row_factory = sqlite3.Row
    dbCursor = memDb.cursor()
    sql = 'BEGIN TRANSACTION;'
    dbCursor.execute(sql)
    sql = 'CREATE TABLE IF NOT EXISTS LINE_ITEMS (\n'
    for index, column in enumerate(extractColumnList):
        dbColumnName = column.replace('/', '_')
        if (fileManifest['fileColumns'][column] == 'String'):
           dbColumnDataType = 'TEXT'
        if (fileManifest['fileColumns'][column] == 'BigDecimal'):
           dbColumnDataType = 'NUMBER'
        if (fileManifest['fileColumns'][column] == 'DateTime'):
           dbColumnDataType = 'TEXT'
        if ((index+1) == len(extractColumnList)):
            sql = sql + dbColumnName + ' ' + dbColumnDataType + ' NOT NULL\n'
        else:
            sql = sql + dbColumnName + ' ' + dbColumnDataType + ' NOT NULL,\n'
    sql = sql + ');'
    dbCursor.execute(sql)
    return memDb

# FLUSH SQLLITE DB IN MEMORY TO DISK
def flushMemoryDatabaseToDisk(memoryDb, account):
    if (os.path.exists(account+'.db')):
        os.remove(account+'.db')
    fileDB = sqlite3.connect(account+'.db')
    with fileDB:
        for line in memoryDb.iterdump():
            if line not in ('BEGIN;', 'COMMIT;'): # let python handle the transactions
                try:
                    fileDB.execute(line)
                except (TypeError, sqlite3.OperationalError) as e:
                    print(line)
                    print(e)
    fileDB.commit()

# INSERT RECORD ON DATABASE
def insertRecord(memoryDB, extractColumnList, columnValues, columnDatatypes, fileManifest):
    sql = 'INSERT INTO LINE_ITEMS (\n'
    for index, column in enumerate(extractColumnList):
        if ((index+1) == len(extractColumnList)):
            sql = sql + column.replace('/','_') + ') VALUES (\n'
        else:
            sql = sql + column.replace('/','_') + ',\n' 
    for index, columnValue in enumerate(columnValues):
        if (fileManifest['fileColumns'][extractColumnList[index]] == 'String'):
            convertedValue = '"' + columnValue + '"' 
        if (fileManifest['fileColumns'][extractColumnList[index]] == 'DateTime'):
            convertedValue = '"' + columnValue.strftime('%Y-%m-%d') + '"' 
        if (fileManifest['fileColumns'][extractColumnList[index]] == 'BigDecimal'):
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
    print(tabulate(result, result[0].keys(), tablefmt='psql', numalign='right', stralign='left'))


# FETCH CSV FILE STRUCTURE FROM JSON MANIFEST
def fetchManifest(cachePath, filename):
    jsonManifestFile = open(cachePath + filename)
    jsonManifest = json.loads(jsonManifestFile.read())
    jsonManifestFile.close()

    manifest = {}
    manifest['account'] = jsonManifest['account']
    fileStructure = {}
    for column in jsonManifest['columns']:
        columnName = column['category'] + '/' + column['name']
        columnDataType = column['type']
        fileStructure[columnName] = columnDataType
    manifest['fileColumns'] = fileStructure
    return manifest

# IMPORT CSV FILE INTO MEMORY DATABASE
def importCsvToDatabase(cachePath, csvFilename, memoryDB, extractColumnList, fileManifest):

    with open(cachePath + csvFilename) as f:
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
            columnDatatype = fileManifest['fileColumns'][field]
            columnDatatypes.append(columnDatatype)
        #print(columnDatatypes)
        # iterate over file and get each field value
        for line in f:
            record = line.split(',')
            columnValues = []
            for field in extractColumnList:
                columnIndex = columnHeader.index(field)
                columnValue = record[columnIndex]
                if (fileManifest['fileColumns'][field] == 'String'):
                    columnValues.append(record[columnIndex])
                if (fileManifest['fileColumns'][field] == 'BigDecimal'):
                    columnValues.append(Decimal(record[columnIndex]))
                if (fileManifest['fileColumns'][field] == 'DateTime'):
                    #columnValues.append(record[columnIndex])
                    columnValues.append(datetime.datetime.strptime(record[columnIndex],'%Y-%m-%dT%H:%M:%SZ'))
            # if (columnValues[0] == 'Usage'):
                #print(columnValues)
            insertRecord(memoryDB, extractColumnList, columnValues, columnDatatypes, fileManifest)

# CREATE SUBDIRECTORIES UNDER CACHE
def makeCacheFolders(filenameWithPath):
    fileSplit = filenameWithPath.split('/')
    startPath = './cache/'
    for index, pathPart in enumerate(fileSplit):
        if ((index+1) < len(fileSplit)):
            startPath = startPath + pathPart + '/'
            if (not os.path.exists(startPath)):
                os.mkdir(startPath)


# LIST CONTENT OF DIRECTORY AND DOWNLOAD
def downloadFilesFromBucket(bucket_name, bucket_path):
    # check if cache exists and create it
    if (not os.path.exists(CACHE_PATH)):
        os.mkdir(CACHE_PATH)
    bucketContents = s3.list_objects(Bucket=bucket_name,Prefix=bucket_path)['Contents']
    downloadFiles = []
    for item in bucketContents:
        filenameWithPath = item['Key']
        filename = Path(filenameWithPath).name
        makeCacheFolders(filenameWithPath)
        s3.download_file(bucket_name, filenameWithPath, CACHE_PATH + filenameWithPath)
        if (filename[-3:] == '.gz'):
            downloadFiles.append(unzipFile(CACHE_PATH, filenameWithPath))
        if (filename[-5:] == '.json'):
            downloadFiles.append(filenameWithPath)

    return downloadFiles

# MAIN FLOW
commandLineResult = commandLineVerification()
if (commandLineResult['status']):
    # INITIALIZE BOTO3
    # choose profile to be used 
    boto3.setup_default_session(profile_name=commandLineResult['--profile'])

    # GLOBAL VARIABLES
    extractColumnList = ['identity/LineItemId', 'lineItem/LineItemType', 'lineItem/UsageStartDate', 'lineItem/UsageEndDate', 'lineItem/ProductCode', \
        'lineItem/UsageType', 'lineItem/Operation', 'lineItem/UsageAmount', 'lineItem/BlendedCost', 'lineItem/UnblendedCost', 'bill/BillingPeriodStartDate', 'lineItem/UsageAccountId', 'bill/InvoiceId']

    s3 = boto3.client('s3')
    downloadedFiles = downloadFilesFromBucket(commandLineResult['--bucket'], commandLineResult['--billing-report-path'])
    print(downloadedFiles)
    fileManifest = fetchManifest(CACHE_PATH,downloadedFiles[1])

    memoryDb = createMemoryDatabase(extractColumnList, fileManifest)
    importCsvToDatabase(CACHE_PATH,downloadedFiles[0], memoryDb, extractColumnList, fileManifest)
    queryDatabase(memoryDb, 'SELECT lineItem_UsageAccountId as ACCOUNT_ID, bill_InvoiceId as INVOICE_ID, min(lineItem_UsageStartDate) as USAGE_START, max(lineItem_UsageEndDate) as USAGE_END, round(sum(lineItem_BlendedCost),2) as TOTAL FROM LINE_ITEMS where lineItem_LineItemType = "Usage" group by lineItem_UsageAccountId, bill_InvoiceId', 'PERIODO')
    queryDatabase(memoryDb, 'SELECT lineItem_LineItemType ITEM_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS GROUP BY lineItem_LineItemType', 'IMPOSTOS')
    queryDatabase(memoryDb, 'SELECT lineItem_ProductCode as PRODUCT_CODE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_ProductCode', 'SERVICOS')
    queryDatabase(memoryDb, 'SELECT lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_UsageType', 'USO')
    queryDatabase(memoryDb, 'SELECT lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_Operation', 'OPERATION')
    #queryDatabase(memoryDb, 'SELECT lineItem_ProductCode as PRODUCT_CODE, lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_ProductCode,lineItem_UsageType', 'USO')
    #queryDatabase(memoryDb, 'SELECT lineItem_ProductCode as PRODUCT_CODE, lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_BlendedCost),2) AS BLENDED_COST FROM LINE_ITEMS where lineItem_LineItemType = "Usage" GROUP BY lineItem_ProductCode,lineItem_Operation', 'OPERATION')
    flushMemoryDatabaseToDisk(memoryDb, fileManifest['account'])
