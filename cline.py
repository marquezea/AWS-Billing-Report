import sys

def checkLineArguments():
    errorCheck = False
    commandLineArguments = []
    for i, arg in enumerate(sys.argv):
        commandLineArguments.append(arg.lower())
        if (arg.lower()[0:2] == '--'):
            if (arg.lower() != '--bucket') and (arg.lower() != '--profile') and (arg.lower() != '--billing-report-path'):
                errorCheck = True
                print('error: unknown parameter ' + arg.lower() + '\nusage: python aws-billing-report.py [--profile <aws-cli-profile-name>] --bucket <bucket-name> --path <path-to-billing-report>')
    if (not errorCheck):
        try:
            bucketName = commandLineArguments[commandLineArguments.index('--bucket')+1]
            print(bucketName) 

            billingReportPath = commandLineArguments[commandLineArguments.index('--billing-report-path')+1]
            print(billingReportPath) 

            try:    
                profile = commandLineArguments[commandLineArguments.index('--profile')+1]
            except:
                profile = 'default'
            print(profile) 
        except:
            print('usage: python aws-billing-report.py [--profile <aws-cli-profile-name>] --bucket <bucket-name> --path <path-to-billing-report>')

checkLineArguments()


# BILLING_REPORT_BUCKET = 'backup-chipr-denis'
# BILLING_REPORT_BUCKET_PATH = 'report/billing_report/20210301-20210401/20210313T184621Z/'
# PROFILE_NAME='denischipr'