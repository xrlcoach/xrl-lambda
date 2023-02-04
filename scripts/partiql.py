import boto3
from boto3.dynamodb.conditions import Key, Attr


db = boto3.client('dynamodb')

results = db.execute_statement(
    Statement='SELECT team_name FROM XRL2021 WHERE "pk" = \'USER#wolvershank\' and "sk" = \'DETAILS\'',
    # Parameters={
    #     ':team': 'team_name',
    #     ':pk': 'USER#wini',
    #     ':sk': 'DETAILS'
    # }
)['Items']

print(results[0]['team_name'])
