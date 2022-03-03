import json
import urllib3
import csv
import os
import boto3
import botocore.exceptions
from datetime import datetime, timedelta

# Set Global variables for Webex, and S3 bucket
webex_token = os.environ['webex_token']
bucket_name = os.environ['bucket_name']
object_key = os.environ['object_key']
URL = 'https://webexapis.com/v1/'
headers = {
    "Authorization": "Bearer " + webex_token,
    "Content-Type": "application/json"
}
http = urllib3.PoolManager()
s3_client = boto3.client('s3')


# Lambda Handler to run desired variables when called, and sort by date/time from the payload
def lambda_handler(event, context):
    body = json.loads(event['body'])
    msg_room_id = body['data']['roomId']
    ts = datetime.today()
    hour = int(ts.strftime("%H"))  # Get current hour
    if hour <= 5:  # If it's before 05:00, reference previous day
        dt = ts - timedelta(days=1)
    else:
        dt = ts
    day_of_week = dt.strftime("%A")
    csv_file = get_csv()
    file = open(csv_file)
    csvreader = csv.reader(file)
    # header = next(csvreader)
    rows = []
    for row in csvreader:
        if row[1].strip() == day_of_week:
            rows.append(row)
    file.close()
    start_time = datetime.strptime(f'{dt.strftime("%m/%d/%Y")} {rows[0][2]}', '%m/%d/%Y %H:%M')
    end_time = start_time + timedelta(hours=int(rows[0][3]))
    sort = f"""
    Hotline for {day_of_week}
    Start: {start_time.strftime('%m/%d/%Y %H:%M')}
    End: {end_time.strftime('%m/%d/%Y %H:%M')}
    Tech: {rows[0][0]}
    Phone: {rows[0][4]}
    """
    webex_send_message(sort, msg_room_id)  # Call function to return an answer to webex


def webex_get_message_details(i_msg_id):  # Get details of message with GET request
    url_route = 'messages'
    try:
        result = http.request('GET',
                              url=URL + url_route + '/' + i_msg_id,
                              headers=headers)
        resp = json.loads(result.data.decode('utf-8'))
    except urllib3.exceptions.HTTPError as e:
        print(e)
    else:
        return resp


def get_csv():  # Grabs csv from from S3 bucket, returns csv and passes to above function for lambda
    local_file = '/tmp/' + object_key
    try:
        s3_client.download_file(bucket_name, object_key, local_file)
        # s3_client.Bucket(bucket_name).download_file(object_key, local_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            pass
        else:
            raise e
    return local_file


def webex_send_message(msg, room_id): # Sends message as POST back to Webex
    url_route = "messages"
    message = {
        "roomId": room_id,
        "markdown": msg
    }
    try:
        result = http.request('POST',
                              url=URL + url_route,
                              headers=headers,
                              body=json.dumps(message))
        print(result)
        resp = json.loads(result.data.decode('utf-8'))
    except urllib3.exceptions.HTTPError as e:
        print(e)
    else:
        return resp
