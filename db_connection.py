import boto3
import psycopg2
import os
import redshift_connector
import base64
from logger_sf import *
import json
from botocore.exceptions import ClientError
from logging import *

LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'w',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

#-----------------------------------------------------------------------------------------------------------------------

def get_secret():

    secret_name = "arn:aws:secretsmanager:ap-southeast-1:0123456789:secret:apsoutheast_practice-L8U2NB"
    region_name = "ap-southeast-1"  
    os.environ['aws_access_key_id'] = 'aws_access_key_id'
    os.environ['aws_secret_access_key'] = 'aws_secret_access_key'

        # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            # print(secret)
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret


#-----------------------------------------------------------------------------------------------------------------------
# DB connection
class DBConnection:
    def __init__(self, secrets): ## These values should be read from AWS Secret Manager - In Secret Manager Password and userid should be encrypted form.
        self.host = secrets["host"]
        self.port = str(secrets["port"])
        self.dbname = secrets["dbname"]
        self.user = secrets["username"]
        self.password = secrets["password"]


    def get_db_connection(self):
        try:
            db_conn = redshift_connector.connect(
                        host=self.host,
                        database=self.dbname,
                        user=self.user,
                        password=self.password
            )
            logger.info("DB Connection Successful")
            # print("Db connection established 1111")
            db_conn.autocommit = True
            return db_conn
        except Exception as e:
            logger.critical("Exception in DB Connection")
            # print(e)
            logger.error(e)

def get_db_conn(secrets):
    logger.info("Inside get_db_conn")
    postgres_db = DBConnection(secrets)
    db_conn = postgres_db.get_db_connection()
    return db_conn

# get_secret()