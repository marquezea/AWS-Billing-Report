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

# CONSTANTS
CACHE_PATH = './cache/'
PARAM_PROFILE = '--profile'
PARAM_BUCKET = '--bucket'
PARAM_VERBOSE = '--verbose'
PARAM_BILLING_REPORT_PATH = '--billing-report-path'

# BILLING_REPORT_BUCKET = 'amarquezelogs'
# BILLING_REPORT_BUCKET_PATH = 'costreport/AMMCostReport/20210201-20210301/20210303T011135Z/'
# PROFILE_NAME='pythonAutomation'
# aws s3 ls s3://amarquezelogs/costreport/AMMCostReport/20210201-20210301/ --profile pythonAutomation

# BILLING_REPORT_BUCKET = 'backup-chipr-denis'
# BILLING_REPORT_BUCKET_PATH = 'report/billing_report/20210301-20210401/20210316T115638Z/'
# PROFILE_NAME='denischipr'
# aws s3 ls s3://backup-chipr-denis/report/billing_report/20210301-20210401/ --profile denischipr

# BILLING_REPORT_BUCKET = 'billing-report-chipr'
# BILLING_REPORT_BUCKET_PATH = 'billing-report/billing-report-chipr/20210301-20210401/20210315T111629Z/'
# PROFILE_NAME='chiprdev'
# aws s3 ls s3://billing-report-chipr/billing-report/billing-report-chipr/20210301-20210401/ --profile chiprdev

# PRINT MESSAGES WITH VERBOSE MODE IS ON
def verbose(verboseMode, message):
    if verboseMode:
        print(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") + ' : aws-billing-report.py : ' + message)

# GET COMMAND LINE ARGUMENTS
def commandLineVerification():
    commandLineResult = {}
    isOk = True
    commandLineArguments = []
    for i, arg in enumerate(sys.argv):
        if (arg.lower() == PARAM_BUCKET) or (arg.lower() == PARAM_PROFILE) or (arg.lower() == PARAM_BILLING_REPORT_PATH) or (arg.lower() == PARAM_VERBOSE):
            commandLineArguments.append(arg.lower())
        else:
            commandLineArguments.append(arg)
        if (arg[0:2] == '--'):
            if (arg.lower() != PARAM_BUCKET) and (arg.lower() != PARAM_PROFILE) and (arg.lower() != PARAM_BILLING_REPORT_PATH) and (arg.lower() != PARAM_VERBOSE):
                isOk = False
                print('error: unknown parameter ' + arg.lower())
                print('usage: python aws-billing-report.py [{0} <aws-cli-profile-name>] {1} <bucket-name> {2} <path-to-billing-report> [{3}]'.format(PARAM_PROFILE,PARAM_BUCKET,PARAM_BILLING_REPORT_PATH,PARAM_VERBOSE))
    if (isOk):
        try:
            bucketName = commandLineArguments[commandLineArguments.index(PARAM_BUCKET)+1]
            commandLineResult[PARAM_BUCKET] = bucketName

            billingReportPath = commandLineArguments[commandLineArguments.index(PARAM_BILLING_REPORT_PATH)+1]
            commandLineResult[PARAM_BILLING_REPORT_PATH] = billingReportPath

            commandLineResult[PARAM_VERBOSE] = (PARAM_VERBOSE in commandLineArguments)
            try:    
                profile = commandLineArguments[commandLineArguments.index(PARAM_PROFILE)+1]
            except:
                profile = 'default'
            commandLineResult[PARAM_PROFILE] = profile
        except:
            print('usage: python aws-billing-report.py [{0} <aws-cli-profile-name>] {1} <bucket-name> {2} <path-to-billing-report> [{3}]'.format(PARAM_PROFILE,PARAM_BUCKET,PARAM_BILLING_REPORT_PATH,PARAM_VERBOSE))
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

    #deleteFile(cachePath + gzFilename)

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
            #print(columnValue, columnValue.strftime('%Y-%m-%d'), columnValue.strftime('%Y-%m-%d %H:%M:%S'))
            convertedValue = '"' + columnValue.strftime('%Y-%m-%d %H:%M:%S') + '"' 
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
def queryDatabase(memoryDB, title, query):
    dbCursor = memoryDB.cursor()
    dbCursor.execute(query)
    result = dbCursor.fetchall()
    print('\n' + title)
    print("=" * len(title))
    if (len(result) > 0):
        print(tabulate(result, result[0].keys(), tablefmt='psql', numalign='right', stralign='left'))
    else:
        print('No data not available for this query')


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
    listResult = s3.list_objects(Bucket=bucket_name,Prefix=bucket_path)
    if ('Contents' in listResult):
        bucketContents = listResult['Contents']
    else:
        bucketContents = []
    downloadFiles = []
    for item in bucketContents:
        filenameWithPath = item['Key']
        filename = Path(filenameWithPath).name
        makeCacheFolders(filenameWithPath)
        if (not os.path.exists(CACHE_PATH + filenameWithPath)):
            verbose(commandLineResult['--verbose'], 'Downloading file {0}{1} from S3 bucket in AWS'.format(CACHE_PATH, filenameWithPath))
            s3.download_file(bucket_name, filenameWithPath, CACHE_PATH + filenameWithPath)
        else:
            verbose(commandLineResult['--verbose'], 'Skipping download of file {0}{1}. File already in local cache.'.format(CACHE_PATH, filenameWithPath))
        if (filename[-3:] == '.gz'):
            downloadFiles.append(unzipFile(CACHE_PATH, filenameWithPath))
        if (filename[-5:] == '.json'):
            downloadFiles.append(filenameWithPath)

    return downloadFiles

# MAIN FLOW
commandLineResult = commandLineVerification()
if (commandLineResult['status']):
    verbose(commandLineResult['--verbose'], 'Starting execution ...')

    # INITIALIZE BOTO3
    # choose profile to be used 
    boto3.setup_default_session(profile_name=commandLineResult[PARAM_PROFILE])

    # GLOBAL VARIABLES
    extractColumnList = ['identity/LineItemId', 'lineItem/LineItemType', 'lineItem/UsageStartDate', 'lineItem/UsageEndDate', 'lineItem/ProductCode', 'lineItem/ResourceId', \
        'lineItem/UsageType', 'lineItem/Operation', 'lineItem/UsageAmount', 'lineItem/BlendedCost', 'lineItem/UnblendedCost', 'bill/BillingPeriodStartDate', 'lineItem/UsageAccountId', 'bill/InvoiceId']

    s3 = boto3.client('s3')
    verbose(commandLineResult['--verbose'], 'Downloading files from S3 bucket ...')
    downloadedFiles = downloadFilesFromBucket(commandLineResult[PARAM_BUCKET], commandLineResult[PARAM_BILLING_REPORT_PATH])

    if (len(downloadedFiles) > 0):
        verbose(commandLineResult['--verbose'], 'Reading manifest from S3 bucket ...')
        fileManifest = fetchManifest(CACHE_PATH,downloadedFiles[1])

        verbose(commandLineResult['--verbose'], 'Creating in memory database ...')
        memoryDb = createMemoryDatabase(extractColumnList, fileManifest)

        verbose(commandLineResult['--verbose'], 'Importing CSV files ...')
        importCsvToDatabase(CACHE_PATH,downloadedFiles[0], memoryDb, extractColumnList, fileManifest)
        verbose(commandLineResult['--verbose'], 'Executing queries and output results ...')

        queryDatabase(memoryDb, 'REPORT PERIOD', 'SELECT lineItem_UsageAccountId as ACCOUNT_ID, bill_InvoiceId as INVOICE_ID, min(strftime(\'%Y-%m-%d\', lineItem_UsageStartDate)) as USAGE_START, max(strftime(\'%Y-%m-%d\', lineItem_UsageEndDate)) as USAGE_END, round(sum(lineItem_UnblendedCost),2) as TOTAL \
            FROM LINE_ITEMS group by lineItem_UsageAccountId, bill_InvoiceId')
        queryDatabase(memoryDb, 'HIGH LEVEL USAGE & COST BY TYPE', 'SELECT lineItem_LineItemType ITEM_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM LINE_ITEMS GROUP BY lineItem_LineItemType', )
        queryDatabase(memoryDb, 'SERVICES COSTS (without Tax)', 'SELECT lineItem_ProductCode as PRODUCT_CODE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_ProductCode')
        queryDatabase(memoryDb, 'RESERVED INSTANCE COSTS', 'SELECT lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType = "RIFee" GROUP BY lineItem_UsageType')
        queryDatabase(memoryDb, 'RESERVED INSTANCE - OPERATIONS', 'SELECT lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType = "RIFee" GROUP BY lineItem_Operation')
        queryDatabase(memoryDb, 'USAGE AND COST (without Tax)', 'SELECT lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_UsageType HAVING round(SUM(lineItem_UsageAmount),2) > 0')
        queryDatabase(memoryDb, 'USAGE AND COSTS OPERATIONS (without Tax)', 'SELECT lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_Operation')
        queryDatabase(memoryDb, 'DAILY COSTS PER SERVICE (without Tax)', 'select lineItem_ProductCode AS PRODUCT_CODE, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate) AS DATE, round(sum(lineItem_UnblendedCost),2) as TOTAL \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY strftime(\'%Y-%m-%d\', lineItem_UsageStartDate), lineItem_ProductCode HAVING round(sum(lineItem_UnblendedCost),2) > 0 ORDER BY lineItem_ProductCode, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate)')
        queryDatabase(memoryDb, 'SUBTOTAL PER PRODUCT AND USAGE TYPE (without Tax)', 'SELECT lineItem_ProductCode as PRODUCT_CODE, lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_ProductCode,lineItem_UsageType HAVING round(SUM(lineItem_UsageAmount),2) > 0', )
        queryDatabase(memoryDb, 'SUBTOTAL PER PRODUCT AND OPERATION (without Tax)', 'SELECT lineItem_ProductCode as PRODUCT_CODE, lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
            FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_ProductCode,lineItem_Operation HAVING round(SUM(lineItem_UsageAmount),2) > 0')
        verbose(commandLineResult['--verbose'], 'Flushing in memory database to sqlite.db ({0})  ...'.format(fileManifest['account']))
        flushMemoryDatabaseToDisk(memoryDb, fileManifest['account'])

    else:
        verbose(commandLineResult['--verbose'], 'No files in the provided bucket and billing report path ...')

