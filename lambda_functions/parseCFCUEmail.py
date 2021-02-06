import os
import sys
import re

import boto3
import botocore
from datetime import datetime

s3client = boto3.client('s3')
BUCKET_NAME = os.getenv('bucket_name')  # S3 bucket of transaction emails

ddbclient = boto3.client('dynamodb')
TABLE_NAME = os.getenv('table_name')  # DynamoDB table of transaction data

NUM_DIGITS = 4  # Last digits of credit card
WS = '(?:\s|&nbsp;)*'  # Whitespace regex


def lambda_handler(event, context):
    '''Get the email describing the transaction, parse it for the transaction
    data, and write that data to DynamoDB.'''
    ses_notification = event['Records'][0]['ses']
    message_id = ses_notification['mail']['messageId']
    try:
        email = s3client.get_object(Bucket=BUCKET_NAME, Key=message_id)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Could not find email. Exit program.
            print('The object does not exist. Key: ' + message_id)
            sys.exit(1)
        else:
            raise
    contents = email['Body'].read().decode('utf-8')
    (last_digits, date, amount, payee) = parse(contents)
    print('Parsed result:', last_digits, date, amount, payee)
    save_to_db(message_id, last_digits, date, amount, payee)


def parse(contents):
    '''Parse the contents of the email for transaction data.'''
    if 'Coastal Alert: A Transaction Has Occurred on Your Account' not in contents:
        print('Email does not match.  Exiting.')
        sys.exit(0)

    print('Email matches. Parsing contents.')

    remainder = re.split(r'Account:{0}'.format(WS), contents, 1)[1]
    last_digits = remainder.split(" ", 1)[0]

    amount_result = re.split(r'Amount:{0}(\(?\$(\d+\.\d\d)\))?'.format(WS), remainder)
    amount_raw = amount_result[1].replace('$', '')
    amount = amount_result[2]

    if amount_raw == amount:
        amount = '-' + amount

    payee = re.split(r'Description:(.*)\n', remainder)[1]

    date = re.split(r'Date:\s(\d{1,2}\s[A-Za-z]{3}\s\d{4})', contents)[1].split(' ')
    date = format_date('{0} {1}, {2}'.format(date[1], date[0], date[2]))

    return last_digits, date, amount, payee


def format_date(date):
    '''Convert dates to ISO 8601 (RFC 3339 "full-date") format.'''
    date = date.replace('\r\n', ' ')
    month_day_year = date.replace(',', '').split(' ')
    month_and_day = datetime.strptime(f'{month_day_year[0]} {month_day_year[1]}', '%b %d')
    year = month_day_year[2]
    month = (f'{month_and_day:%m}')  # formats datetime.month object to 2 digit string
    day = (f'{month_and_day:%d}')  # formats datetime.day object to 2 digit string
    return '{0}-{1}-{2}'.format(year, month, day)


def save_to_db(message_id, last_digits, date, amount, payee):
    ddbclient.put_item(TableName=TABLE_NAME,
                       Item={'message_id': {'S': message_id},
                             'last_digits': {'S': last_digits},
                             'amount': {'S': amount},
                             'payee': {'S': payee},
                             'date': {'S': date}
                             },
                       ConditionExpression='attribute_not_exists(message_id)')
