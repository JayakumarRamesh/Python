import boto3
import psycopg2
import os
import redshift_connector
import base64
from db_connection import *
import json
from botocore.exceptions import ClientError
from logging import *
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = os.path.join(BASE_DIR, "PREV_FILE\logfile.log")


LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'r+',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

# secrets = get_secret()
# secrets = secrets.replace("\n", "")
# secrets = secrets.replace(" ", "")
# secrets = json.loads(secrets)
# s3_client = boto3.client("s3", region_name=secrets["region_name"], aws_access_key_id=secrets["aws_access_key_id"], aws_secret_access_key=secrets["aws_secret_access_key"])


def upload_log(bucket_name):
    logger.info("Inside upload_log function")
    folder = 'logfile/' + 'logfile.log'
    s3_client = boto3.client('s3')
    print('Inside upload log')
    try:
        s3_client.upload_file(LOG_DIR, bucket_name, folder)
        print('Upload Successfully')
        logger.info("Log File Uploaded Successfully!!.")
    except Exception as e:
        print('LogFile Uploaded Failed!!.')
        logger.error("LogFile Uploaded Failed!!.")
        logger.error(e)

# upload_log('index-bucket-cmp')