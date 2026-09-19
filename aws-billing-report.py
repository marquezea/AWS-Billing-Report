import boto3
import os
import csv
import gzip
import json
import sqlite3
import datetime
import sys
from pathlib import Path
from tabulate import tabulate
from termgraph import termgraph as tg

# CONSTANTS
CACHE_PATH = './cache/'
PARAM_PROFILE = '--profile'
PARAM_BUCKET = '--bucket'
PARAM_VERBOSE = '--verbose'
PARAM_BILLING_REPORT_PATH = '--billing-report-path'

# THE TWO AWS EXPORT FORMATS THIS TOOL UNDERSTANDS
# CUR1 IS THE LEGACY COST AND USAGE REPORT (GZIPPED CSV PARTS PLUS A JSON MANIFEST)
# CUR2 IS THE DATA EXPORTS / CUR 2.0 REPORT (SNAPPY COMPRESSED PARQUET PARTS, NO MANIFEST NEEDED)
FORMAT_CUR1 = 'cur1'
FORMAT_CUR2 = 'cur2'

# EVERY FIELD THE REPORT NEEDS, AND WHERE TO FIND IT IN EACH FORMAT
# dbColumn      : the column name inside the LINE_ITEMS table, which every query below relies on
# csvColumn     : the header name in a CUR1 csv part
# parquetColumn : the column name in a CUR2 parquet part. CUR2 keeps the rarely used product
#                 attributes inside a single 'product' map column instead of one column each,
#                 so 'product:<key>' means "look <key> up in that map"
# dataType      : TEXT, NUMBER or DATETIME (a DATETIME is stored as TEXT, like the csv always was)
FIELDS = [
    ('identity_LineItemId',         'identity/LineItemId',          'identity_line_item_id',          'TEXT'),
    ('lineItem_LineItemType',       'lineItem/LineItemType',        'line_item_line_item_type',       'TEXT'),
    ('lineItem_UsageStartDate',     'lineItem/UsageStartDate',      'line_item_usage_start_date',     'DATETIME'),
    ('lineItem_UsageEndDate',       'lineItem/UsageEndDate',        'line_item_usage_end_date',       'DATETIME'),
    ('product_ProductName',         'product/ProductName',          'product:product_name',           'TEXT'),
    ('lineItem_UsageType',          'lineItem/UsageType',           'line_item_usage_type',           'TEXT'),
    ('lineItem_Operation',          'lineItem/Operation',           'line_item_operation',            'TEXT'),
    ('lineItem_UsageAmount',        'lineItem/UsageAmount',         'line_item_usage_amount',         'NUMBER'),
    ('lineItem_BlendedCost',        'lineItem/BlendedCost',         'line_item_blended_cost',         'NUMBER'),
    ('lineItem_UnblendedCost',      'lineItem/UnblendedCost',       'line_item_unblended_cost',       'NUMBER'),
    ('bill_BillingPeriodStartDate', 'bill/BillingPeriodStartDate',  'bill_billing_period_start_date', 'DATETIME'),
    ('lineItem_UsageAccountId',     'lineItem/UsageAccountId',      'line_item_usage_account_id',     'TEXT'),
    ('bill_InvoiceId',              'bill/InvoiceId',               'bill_invoice_id',                'TEXT'),
]

# POSITIONS INSIDE A FIELDS ENTRY
DB_COLUMN = 0
CSV_COLUMN = 1
PARQUET_COLUMN = 2
DATA_TYPE = 3

# HOW A CUR1 CSV WRITES ITS TIMESTAMPS
CSV_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# THE CUR2 MAP COLUMN THAT HOLDS THE PRODUCT ATTRIBUTES, AND THE PREFIX THAT POINTS INTO IT
PRODUCT_MAP_COLUMN = 'product'
PRODUCT_MAP_PREFIX = 'product:'

# CUR1 TAKES THE ACCOUNT FROM ITS MANIFEST, CUR2 HAS NO MANIFEST SO IT COMES FROM THE PAYER COLUMN
PARQUET_ACCOUNT_COLUMN = 'bill_payer_account_id'

# USED FOR THE .db FILENAME WHEN THE EXPORT DOES NOT NAME AN ACCOUNT
UNKNOWN_ACCOUNT = 'billing-report'

# command
# python aws-billing-report.py --bucket BUCKET --profile PROFILE --billing-report-path BILLING_PATH --verbose 

# BILLING_REPORT_BUCKET = 'amarquezelogs'
# BILLING_REPORT_BUCKET_PATH = 'costreport/AMMCostReport/20260901-20261001/20260919T105921Z/'
# PROFILE_NAME='pythonAutomation'
# aws s3 ls s3://amarquezelogs/costreport/AMMCostReport/20260901-20261001/20260919T105921Z/ --profile pythonAutomation
# python aws-billing-report.py --bucket amarquezelogs --profile pythonAutomation --billing-report-path costreport/AMMCostReport/20260901-20261001/20260919T105921Z/ --verbose > 2026-09.txt

# BILLING_REPORT_BUCKET = 'amm-account-admin'
# BILLING_REPORT_BUCKET_PATH = 'daily/cost-export/data/BILLING_PERIOD=2026-09/'
# PROFILE_NAME='admin-master'
# aws s3 ls s3://amm-account-admin/daily/cost-export/data/BILLING_PERIOD=2026-09/ --profile admin-master
# python aws-billing-report.py --bucket amm-account-admin --profile admin-master --billing-report-path daily/cost-export/data/BILLING_PERIOD=2026-09/ --verbose > 2026-09.txt


# OUTPUT EXECUTION INFORMATION WHEN VERBOSE MODE IS ON
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
# THE TABLE IS BUILT FROM THE FIELDS TABLE, SO BOTH EXPORT FORMATS LAND IN THE SAME SHAPE
def createMemoryDatabase():
    memDb = sqlite3.connect(':memory:')
    memDb.row_factory = sqlite3.Row
    columnDefinitions = []
    for field in FIELDS:
        dbColumnDataType = 'NUMBER' if (field[DATA_TYPE] == 'NUMBER') else 'TEXT'
        columnDefinitions.append(field[DB_COLUMN] + ' ' + dbColumnDataType)
    memDb.execute('CREATE TABLE IF NOT EXISTS LINE_ITEMS (\n' + ',\n'.join(columnDefinitions) + '\n);')
    return memDb

# CONVERT ONE RAW VALUE INTO WHAT SQLITE SHOULD STORE
# a missing value keeps the convention the csv export always used: an empty string, or zero for
# a number. CUR2 writes real nulls where the csv wrote empty strings (tax lines have no usage
# type or operation, for example), so without this the two formats would group differently
def convertValue(rawValue, dataType, datetimeFormat):
    if (rawValue is None) or (rawValue == ''):
        return 0.0 if (dataType == 'NUMBER') else ''
    if (dataType == 'NUMBER'):
        return float(rawValue)
    if (dataType == 'DATETIME'):
        # the csv hands us a string to parse, parquet hands us a datetime already
        if (datetimeFormat is not None):
            rawValue = datetime.datetime.strptime(rawValue, datetimeFormat)
        return rawValue.strftime('%Y-%m-%d %H:%M:%S')
    return rawValue

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

# INSERT ALL LINE ITEMS ON DATABASE IN ONE GO
# the values are bound as parameters rather than pasted into the sql text, so a product name
# containing a quote cannot break the statement, and 20k rows load in one round trip
def insertRows(memoryDB, rows):
    sql = 'INSERT INTO LINE_ITEMS (' + ', '.join([field[DB_COLUMN] for field in FIELDS]) + ') ' \
        + 'VALUES (' + ', '.join(['?'] * len(FIELDS)) + ')'
    memoryDB.executemany(sql, rows)
    memoryDB.commit()

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


# QUERY DATABASE AND OUTPUT A PIVOT TABLE WITH ONE ROW PER PRODUCT CODE AND ONE COLUMN PER DAY
def pivotDailyCostPerService(memoryDB, title, query):
    dbCursor = memoryDB.cursor()
    dbCursor.execute(query)
    result = dbCursor.fetchall()
    print('\n' + title)
    print("=" * len(title))
    if (len(result) == 0):
        print('No data not available for this query')
        return

    # PIVOT THE LINES INTO A COST PER PRODUCT CODE PER DAY MATRIX
    costMatrix = {}
    reportDays = set()
    for record in result:
        productCode = record['PRODUCT_CODE']
        reportDay = record['DATE']
        reportDays.add(reportDay)
        if (productCode not in costMatrix):
            costMatrix[productCode] = {}
        costMatrix[productCode][reportDay] = costMatrix[productCode].get(reportDay, 0) + record['TOTAL']
    reportDays = sorted(reportDays)

    # SKIP THE PRODUCT CODES WITHOUT COST IN THE WHOLE PERIOD (FREE TIER USAGE) AND PUT THE BIGGEST SPENDERS FIRST
    productTotals = {}
    for productCode in costMatrix:
        productTotals[productCode] = sum(costMatrix[productCode].values())
    productCodes = [productCode for productCode in costMatrix if round(productTotals[productCode],2) != 0]
    productCodes.sort(key=lambda productCode: productTotals[productCode], reverse=True)
    if (len(productCodes) == 0):
        print('No data not available for this query')
        return

    # ONE COLUMN PER DAY OF THE MONTH, PLUS THE PRODUCT CODE AND THE SUMMARY OF EACH SERVICE
    columnHeader = ['PRODUCT_CODE'] + [reportDay[-2:] for reportDay in reportDays] + ['TOTAL']
    tableRows = []
    for productCode in productCodes:
        tableRow = [productCode]
        for reportDay in reportDays:
            tableRow.append(costMatrix[productCode].get(reportDay, None))
        tableRow.append(productTotals[productCode])
        tableRows.append(tableRow)

    # SUMMARY OF EACH DAY ON THE LAST ROW
    totalRow = ['TOTAL']
    for reportDay in reportDays:
        totalRow.append(sum([costMatrix[productCode].get(reportDay, 0) for productCode in productCodes], 0.0))
    totalRow.append(sum([productTotals[productCode] for productCode in productCodes], 0.0))
    tableRows.append(totalRow)

    print(tabulate(tableRows, columnHeader, tablefmt='psql', floatfmt='.2f', missingval='', numalign='right', stralign='left'))

# FETCH THE ACCOUNT THE REPORT BELONGS TO FROM THE JSON MANIFEST OF A CUR1 EXPORT
# the manifest also describes the csv column types, but the FIELDS table above now does that
# for both formats, so the manifest is only consulted for the account
def fetchManifestAccount(cachePath, filename):
    with open(cachePath + filename) as jsonManifestFile:
        jsonManifest = json.loads(jsonManifestFile.read())
    return jsonManifest.get('account', '')

# READ THE CSV PARTS OF A CUR1 EXPORT INTO ROWS THAT MATCH THE FIELDS TABLE
def loadCsvReport(cachePath, downloadedFiles, verboseMode):
    account = fetchManifestAccount(cachePath, downloadedFiles['manifestFile'])
    rows = []
    # aws splits a big report in several csv parts, all of them belong to the same report
    for index, csvFilename in enumerate(downloadedFiles['dataFiles']):
        verbose(verboseMode, 'Importing CSV file {0} of {1} ({2}) ...'.format(index+1, len(downloadedFiles['dataFiles']), csvFilename))
        with open(cachePath + csvFilename, newline='') as csvFile:
            # the report quotes any field containing a comma, so let the csv module split the records
            csvReader = csv.reader(csvFile)
            # read header line with the column names and find each field we care about once
            columnHeader = next(csvReader)
            columnIndexes = [columnHeader.index(field[CSV_COLUMN]) for field in FIELDS]
            for record in csvReader:
                rows.append(tuple(
                    convertValue(record[columnIndexes[fieldIndex]], field[DATA_TYPE], CSV_DATETIME_FORMAT)
                    for fieldIndex, field in enumerate(FIELDS)))
    return account, rows

# READ THE PARQUET PARTS OF A CUR2 EXPORT INTO ROWS THAT MATCH THE FIELDS TABLE
def loadParquetReport(cachePath, downloadedFiles, verboseMode):
    # pyarrow is only needed for the new format, so the csv path keeps working without it
    try:
        import pyarrow.parquet as parquet
    except ImportError:
        print('error: reading a CUR 2.0 (parquet) export needs pyarrow. Install it with: pip install pyarrow')
        return '', []

    # split the fields into plain columns and lookups inside the 'product' map column
    productKeys = {}
    plainColumns = []
    for fieldIndex, field in enumerate(FIELDS):
        if field[PARQUET_COLUMN].startswith(PRODUCT_MAP_PREFIX):
            productKeys[fieldIndex] = field[PARQUET_COLUMN][len(PRODUCT_MAP_PREFIX):]
        else:
            plainColumns.append(field[PARQUET_COLUMN])
    readColumns = plainColumns + [PARQUET_ACCOUNT_COLUMN]
    if (len(productKeys) > 0):
        readColumns.append(PRODUCT_MAP_COLUMN)
    readColumns = sorted(set(readColumns))

    account = ''
    rows = []
    # aws splits a big report in several parquet parts, all of them belong to the same report
    for index, parquetFilename in enumerate(downloadedFiles['dataFiles']):
        verbose(verboseMode, 'Importing parquet file {0} of {1} ({2}) ...'.format(index+1, len(downloadedFiles['dataFiles']), parquetFilename))
        # parquet is columnar, so asking for 14 of its 120+ columns only reads those off disk
        table = parquet.read_table(cachePath + parquetFilename, columns=readColumns)
        columnValues = {column: table.column(column).to_pylist() for column in readColumns}
        productMaps = []
        if (len(productKeys) > 0):
            productMaps = [dict(entry) if entry else {} for entry in columnValues[PRODUCT_MAP_COLUMN]]
        for rowIndex in range(table.num_rows):
            if (account == '') and columnValues[PARQUET_ACCOUNT_COLUMN][rowIndex]:
                account = columnValues[PARQUET_ACCOUNT_COLUMN][rowIndex]
            row = []
            for fieldIndex, field in enumerate(FIELDS):
                if (fieldIndex in productKeys):
                    rawValue = productMaps[rowIndex].get(productKeys[fieldIndex])
                else:
                    rawValue = columnValues[field[PARQUET_COLUMN]][rowIndex]
                row.append(convertValue(rawValue, field[DATA_TYPE], None))
            rows.append(tuple(row))
    return account, rows

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
def downloadFilesFromBucket(s3, bucket_name, bucket_path, verboseMode):
    # check if cache exists and create it
    if (not os.path.exists(CACHE_PATH)):
        os.mkdir(CACHE_PATH)

    # a big export is split in many parts, so page through the whole prefix instead of
    # taking only the first thousand keys a single list call would return
    bucketKeys = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket_name, Prefix=bucket_path):
        for item in page.get('Contents', []):
            bucketKeys.append(item['Key'])

    # the format is decided by what aws actually delivered, so no extra command line switch
    # is needed: parquet parts mean a CUR2 export, anything else is treated as the old CUR1
    downloadFiles = {'format': FORMAT_CUR1, 'dataFiles': [], 'manifestFile': ''}
    if any(key.endswith('.parquet') for key in bucketKeys):
        downloadFiles['format'] = FORMAT_CUR2

    for filenameWithPath in bucketKeys:
        filename = Path(filenameWithPath).name
        isParquet = filename.endswith('.parquet')
        isGzip = filename.endswith('.gz')
        isManifest = filename.endswith('.json')
        # each format ships its own companion files, only fetch the ones this format needs.
        # a CUR2 export also writes json metadata next to the data, which we have no use for
        if (downloadFiles['format'] == FORMAT_CUR2) and (not isParquet):
            continue
        if (downloadFiles['format'] == FORMAT_CUR1) and (not isGzip) and (not isManifest):
            continue
        makeCacheFolders(filenameWithPath)
        if (not os.path.exists(CACHE_PATH + filenameWithPath)):
            verbose(verboseMode, 'Downloading file {0}{1} from S3 bucket in AWS'.format(CACHE_PATH, filenameWithPath))
            s3.download_file(bucket_name, filenameWithPath, CACHE_PATH + filenameWithPath)
        else:
            verbose(verboseMode, 'Skipping download of file {0}{1}. File already in local cache.'.format(CACHE_PATH, filenameWithPath))
        if isParquet:
            # parquet compresses itself internally, there is nothing to unzip
            downloadFiles['dataFiles'].append(filenameWithPath)
        if isGzip:
            downloadFiles['dataFiles'].append(unzipFile(CACHE_PATH, filenameWithPath))
        if isManifest:
            downloadFiles['manifestFile'] = filenameWithPath

    # keep the parts in the order aws numbered them
    downloadFiles['dataFiles'].sort()
    return downloadFiles

# RUN EVERY REPORT AGAINST THE LOADED LINE ITEMS
# these are the same for both export formats, because both land in the same LINE_ITEMS table
def runReports(memoryDb):
    queryDatabase(memoryDb, 'REPORT PERIOD', 'SELECT lineItem_UsageAccountId as ACCOUNT_ID, bill_InvoiceId as INVOICE_ID, min(strftime(\'%Y-%m-%d\', lineItem_UsageStartDate)) as USAGE_START, max(strftime(\'%Y-%m-%d\', lineItem_UsageEndDate)) as USAGE_END, round(sum(lineItem_UnblendedCost),2) as TOTAL \
        FROM LINE_ITEMS group by lineItem_UsageAccountId, bill_InvoiceId')
    queryDatabase(memoryDb, 'HIGH LEVEL USAGE & COST BY TYPE', 'SELECT lineItem_LineItemType ITEM_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM LINE_ITEMS GROUP BY lineItem_LineItemType', )
    queryDatabase(memoryDb, 'SERVICES COSTS (without Tax)', 'SELECT product_ProductName as PRODUCT_CODE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY product_ProductName')
    queryDatabase(memoryDb, 'RESERVED INSTANCE COSTS', 'SELECT lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType = "RIFee" GROUP BY lineItem_UsageType')
    queryDatabase(memoryDb, 'RESERVED INSTANCE - OPERATIONS', 'SELECT lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType = "RIFee" GROUP BY lineItem_Operation')
    queryDatabase(memoryDb, 'USAGE AND COST (without Tax)', 'SELECT lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_UsageType HAVING round(SUM(lineItem_UsageAmount),2) > 0')
    queryDatabase(memoryDb, 'USAGE AND COSTS OPERATIONS (without Tax)', 'SELECT lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY lineItem_Operation')
    queryDatabase(memoryDb, 'DAILY COSTS PER SERVICE (without Tax)', 'select product_ProductName AS PRODUCT_CODE, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate) AS DATE, round(sum(lineItem_UnblendedCost),2) as TOTAL \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY strftime(\'%Y-%m-%d\', lineItem_UsageStartDate), product_ProductName HAVING round(sum(lineItem_UnblendedCost),2) > 0 ORDER BY product_ProductName, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate)')
    pivotDailyCostPerService(memoryDb, 'DAILY COSTS PER SERVICE - PIVOT (without Tax)', 'SELECT product_ProductName AS PRODUCT_CODE, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate) AS DATE, sum(lineItem_UnblendedCost) as TOTAL \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY product_ProductName, strftime(\'%Y-%m-%d\', lineItem_UsageStartDate)')
    queryDatabase(memoryDb, 'SUBTOTAL PER PRODUCT AND USAGE TYPE (without Tax)', 'SELECT product_ProductName as PRODUCT_CODE, lineItem_UsageType as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY product_ProductName,lineItem_UsageType HAVING round(SUM(lineItem_UsageAmount),2) > 0', )
    queryDatabase(memoryDb, 'SUBTOTAL PER PRODUCT AND OPERATION (without Tax)', 'SELECT product_ProductName as PRODUCT_CODE, lineItem_Operation as USAGE_TYPE, round(SUM(lineItem_UsageAmount),2) AS USAGE_AMOUNT, round(SUM(lineItem_UnblendedCost),2) AS BLENDED_COST \
        FROM line_items WHERE lineItem_LineItemType <> "Tax" GROUP BY product_ProductName,lineItem_Operation HAVING round(SUM(lineItem_UsageAmount),2) > 0')

# MAIN FLOW
def main():
    commandLineResult = commandLineVerification()
    if (not commandLineResult['status']):
        return
    verboseMode = commandLineResult[PARAM_VERBOSE]
    verbose(verboseMode, 'Starting execution ...')

    # INITIALIZE BOTO3
    # choose profile to be used
    boto3.setup_default_session(profile_name=commandLineResult[PARAM_PROFILE])
    s3 = boto3.client('s3')

    verbose(verboseMode, 'Downloading files from S3 bucket ...')
    downloadedFiles = downloadFilesFromBucket(s3, commandLineResult[PARAM_BUCKET], commandLineResult[PARAM_BILLING_REPORT_PATH], verboseMode)

    if (len(downloadedFiles['dataFiles']) == 0):
        verbose(verboseMode, 'No files in the provided bucket and billing report path ...')
        return
    if (downloadedFiles['format'] == FORMAT_CUR1) and (downloadedFiles['manifestFile'] == ''):
        verbose(verboseMode, 'No manifest file next to the CSV parts in the provided billing report path ...')
        return

    if (downloadedFiles['format'] == FORMAT_CUR2):
        verbose(verboseMode, 'Detected a CUR 2.0 (parquet) export ...')
        account, rows = loadParquetReport(CACHE_PATH, downloadedFiles, verboseMode)
    else:
        verbose(verboseMode, 'Detected a CUR v1 (csv) export ...')
        account, rows = loadCsvReport(CACHE_PATH, downloadedFiles, verboseMode)

    if (len(rows) == 0):
        verbose(verboseMode, 'No line items found in the downloaded files ...')
        return

    verbose(verboseMode, 'Creating in memory database ...')
    memoryDb = createMemoryDatabase()
    verbose(verboseMode, 'Inserting {0} line items into the in memory database ...'.format(len(rows)))
    insertRows(memoryDb, rows)

    verbose(verboseMode, 'Executing queries and output results ...')
    runReports(memoryDb)

    if (account == ''):
        account = UNKNOWN_ACCOUNT
    verbose(verboseMode, 'Flushing in memory database to sqlite.db ({0})  ...'.format(account))
    flushMemoryDatabaseToDisk(memoryDb, account)

if (__name__ == '__main__'):
    main()

