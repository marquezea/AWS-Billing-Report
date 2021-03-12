# ALPHA VERSION - do not use

# AWS-Billing-Report
Uma solução para pegar os relatório de Billing da AWS e produzir relatório consolidado de utilização dos serviços. English: A solution to get AWS Billing Report files from S3 and generate automatica visualization for cost exploration

# PRE-REQUISITOS DE SOFTWARE
- Python 3.7.4
- Boto3 1.12.43

# PRE-REQUISITOS CONTA AWS
1) Conta AWS com usuario 'pythonAutomation' com role de 'S3 Full Access'
2) Configurar AWS CLI (aws configure) com perfil 'pythonAutomation'
   aws configure set region <region> --profile pythonAutomation
    
