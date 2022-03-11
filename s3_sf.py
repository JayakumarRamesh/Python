import boto3
import psycopg2
import os
import redshift_connector
import base64
from logger_sf import *
from db_connection import *
import json
from botocore.exceptions import ClientError
from logging import *
import os

# GLOBAL INITIALIZATION
s3_bucket = []
table_list = []
split_table = []



# AWS SECRETE MANAGER

secrets = get_secret()
secrets = secrets.replace("\n", "")
secrets = secrets.replace(" ", "")
secrets = json.loads(secrets)
# print(secrets["region_name"])


# MAIN FUNCTION

if __name__ == '__main__':
    try:        
        logger.info("-----AWS S3 Connectivity Intiated-----")
        logger.info("Setting Up S3 client")
        s3_client = boto3.client("s3", region_name=secrets["region_name"], aws_access_key_id=secrets["aws_access_key_id"], aws_secret_access_key=secrets["aws_secret_access_key"])
        logger.info("Setting Up Os.environ")        
        s3 = boto3.resource('s3')

        # APPENDING ALL BUCKET NAMES IN A LIST
        try:
            logger.info('checking for all the buckets')
            for bucket in s3.buckets.all():
                s3_bucket.append(bucket.name)
            logger.info('Appended all the buckets to s3_bucket list')
        except Exception as e:
            logger.error("Unable to fetch S3 bucket")
            logger.error(e)        
        
        
        # Read the index.txt file from index-bucket-cmp bucket
        try:
            if 'snowdataapsoutheast' in s3_bucket:
                logger.info(" parquet-bucket found in the s3_bucket list")
                parquet_bucket1 = s3.Bucket('snowdataapsoutheast')
                logger.info("Checking for parquet file and table")
                table_input = input("Enter the table name to be processed : ")
                schema_input = input("Enter the schema name to be processed : ")
                
                #TESTING
                for obj1 in parquet_bucket1.objects.all():
                    key1 = obj1.key
                    if key1.startswith(table_input):
                        table_list.append(key1)
                    if key1.split('/')[0] not in table_list:
                        split_table.append(key1.split('/')[0])
                
                print("Table List : ",table_list)
                logger.info("Table List : ")
                logger.info(table_list)
                
                print("split Table : ", split_table)
                logger.info("split Table : ")
                logger.info(split_table)
                
                if len(table_list) == 0:
                    print("Interrupted !! Wrong Table or Schema name ")
                    logger.info("Interrupted !! Wrong Table or Schema name ")
    
                count_array = []
                for i, j in zip(split_table, table_list):
                    print(i, j)
                    if j.endswith('.parquet'):
                        count_array.append('T')
                    else:
                        count_array.append('F')
                n= len(count_array)
                
                print(count_array)
                logger.info(count_array)
                
                con = get_db_conn(secrets)
                cur = con.cursor()
                print('PASSED')
                logger.info("PASSED")
                
                for i in range(n):
                    if(count_array[i]=='T'):
                        table = split_table[i]
                        parquet_buck = table_list[i]
                        print(table,':',parquet_buck)
                        print("copy started ",parquet_buck)
                        logger.info("Copy started for " + str(parquet_buck))
                        copy_command = ("COPY "+schema_input+'.'+table +" FROM '"+'s3://snowdataapsoutheast/'+parquet_buck+"' IAM_ROLE '"+"arn:aws:iam::958785264664:role/redshiftpractiserole'"+"FORMAT AS PARQUET;")
                        cur.execute(copy_command)
                        print('copying completed',parquet_buck)
                        logger.info("Copy completed for " + str(parquet_buck))
                        con.commit()
            else:
                logger.error('S3 Bucket Not Found-----!!')
                print("writing in log file s3 bucket not found")
        except Exception as e:
            logger.error("Something went wrong while processing  parquet bucket----->")

    except Exception as e:
        logger.critical("Main Execution Stopped----->")

    finally:
        logger.info("Job Executed------------------------------------------------------------------------------------------------------------------")
        upload_log('snowlogapsoutheast')
     