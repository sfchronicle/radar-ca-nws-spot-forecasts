import requests
import pandas as pd
import json
from datetime import datetime
import pytz
import os
import configparser
import boto3

# Parse the config 
cp = configparser.ConfigParser(interpolation=None)

# read env file based on location
if os.path.exists('../../.env'):  
    cp.read('../../.env')  
elif os.path.exists('/Users/emma.stiefel/.env'):
    cp.read('/Users/emma.stiefel/.env')  
else: # production
    cp.read("/home/ec2-user/Projects/deploy-engine/.env")

os.environ["SFC_AWS_ACCESS_KEY_ID"] = cp.get('aws', 's3_user')
os.environ["SFC_AWS_SECRET_ACCESS_KEY"] = cp.get('aws', 's3_pass')
s3 = boto3.resource('s3')

sfc_bucket_string = 'sfc-project-files'
sfc_client = boto3.client('s3',
    aws_access_key_id=os.environ['SFC_AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['SFC_AWS_SECRET_ACCESS_KEY']
)

# send message to slack
webhook_url = "https://hooks.slack.com/services/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"
def send_message(message):
    # print(message)
    message_dict = {
        "text": message
    }
    r = requests.post(
        webhook_url, 
        data=json.dumps(message_dict),
        headers={'Content-Type': 'application/json'}
        )
        
try:
    # read in existing data
    df = pd.read_json("https://files.sfchronicle.com/radar-ca-nws-spot-forecasts/spot_forecasts.json")

    # load in and process new data
    url = 'https://spot.weather.gov/cms/api/1.0/requests?office.id=&isArchived=false'
    r = requests.get(url)

    new_df = pd.DataFrame(r.json())

    new_df['wfo'] = [o['nativeSiteId'] for o in new_df['office']]
    new_df['name'] = new_df['projectName']
    new_df['type'] = [i['name'] for i in new_df['incident']]
    new_df['submission_time'] = new_df['submittedAt']
    new_df['deliver_time'] = new_df['deliverAt']

    new_df = new_df[['id', 'name', 'type', 'submission_time', 'deliver_time', 'wfo']]

    # just the offices we want
    offices = ["MTR", "STO", "HNX", "LOX", "SGX", "VEF", "REV", "MTR", "EKA"]
    new_df = new_df[[o in offices for o in new_df['wfo']]]

    # find forecasts that are new, not in existing df
    new_forecasts = new_df[[i not in list(df['id']) for i in new_df['id']]]
        
    # send alerts for each new forecast
    tz = pytz.timezone('US/Pacific')
    for i, r in new_forecasts.iterrows(): 
        # convert times
        s = datetime.fromisoformat(r['submission_time'])
        s = s.astimezone(tz)
        s = s.strftime('%m/%d/%25 %I:%M%p')

        d = datetime.fromisoformat(r['deliver_time'])
        d = d.astimezone(tz)
        d = d.strftime('%m/%d/%25 %I:%M%p')
        send_message(f"New spot forecast request\nName: {r['name']}\nType: {r['type']}\nWFO: {r['wfo']}\nSubmitted: {s}\nDelivered: {d}")

    # save new df as old df
    # new_df.to_csv('spot_forecasts.csv')
    sfc_client.put_object(Body=new_df.to_json(), Bucket=sfc_bucket_string, Key="radar-ca-nws-spot-forecasts/spot_forecasts.json")

except Exception as E:
    send_message(f'scraping ERROR {E}')