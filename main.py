import traceback
import boto3
import hashlib
from datetime import datetime
import argparse
import logging
import sys
import os

# Creating and Configuring Logger
logfile = "FDW_MD5Validation_" + str(datetime.now().strftime('%Y-%m-%d_%H-%M-%S')) + '.log'
# Creating and Configuring Logger

Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename=logfile,
                    # stream=sys.stdout,
                    filemode="w",
                    format=Log_Format,
                    level=logging.INFO)

logger = logging.getLogger()

# Declaring S3 Client and region
logger.info("Initializing S3 Client...")

AWS_REGION = "ap-southeast-1"

s3_client = boto3.client("s3", region_name=AWS_REGION, aws_access_key_id='AKIA56PAODQMMWPZVKGU',
                         aws_secret_access_key='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')

# creating parser to get cmd arguments
parser = argparse.ArgumentParser()
# Add long and short argument
parser.add_argument("--bucket_name", "-bucket", help="set bucket name")
parser.add_argument("--file_name", "-filename", help="set bucket name")

# Read arguments from the command line
args = parser.parse_args()


# MD5 CheckSum method based on LLD
def md5_checksum(response):
    md5_hash = hashlib.md5()
    for i, chunk in enumerate(response['Body'].iter_chunks(1024 * 1024)):
        md5_hash.update(chunk)
    return md5_hash.hexdigest()


# MD5 CheckSum method based on S3 etag
# def get_s3_etag(s3_object):
#     s3obj_etag = s3_object['Etag'].strip('"')
#     logger.info(s3obj_etag)
#     #return s3obj_etag


# Upload log file to S3
def upload_log(bucket_name):
    folder = 'logfile/' + logfile
    try:
        s3_client.upload_file(logfile, bucket_name, folder)
        logger.info("Log File Uploaded Successfully!!.")
    except Exception as e:
        logger.error("LogFile Uploaded Failed!!.")
        logger.error(e)


logger.info("Reading CMD arguments.....")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        # Check for --bucket_name
        if True:
        #if args.bucket_name:
            logger.info("The given bucket name in arguments is %s" % args.bucket_name)
            # bucket_name = args.bucket_name
            bucket_name = "statefarm-data-migration-snowball-testing"
            try:
                # Check for --file_name
                if True:
                #if args.file_name:
                    logger.info("The given index file name in arguments is %s" % args.file_name)
                    #file_name = str(args.file_name)
                    file_name = 'MD5Lambda_Index_2022-02-12_04-22-10.txt'
                    # Declaring Output file name
                    output_filename = 'processed_' + file_name
                    # Creating new Output file
                    f = open(output_filename, "w")
                    f.close()
                    # Checking if the index file exists in the path and file is not empty
                    if os.path.isfile('./' + file_name) and os.stat('./' + file_name).st_size != 0:
                        logger.info(
                            "The given index file {0} exists in the path and is not empty".format(str(file_name)))
                        index_file_contents = open(file_name, 'r').read()
                        file_list = index_file_contents.split(',')
                        file_count = len(file_list)
                        logger.info("Number of files in the given index file : " + str(file_count))
                        # Iterating the files
                        for i, objkey in enumerate(file_list):
                            file_no = i + 1
                            logger.info("Processing file : {0}/{1}".format(str(file_no), str(file_count)))
                            s3_obj = objkey.split('|')[0]
                            on_prem_md5 = objkey.split('|')[1]
                            # Initializing new dict to add processed MD5 details..
                            processed_md5 = {}
                            key = s3_obj
                            logger.info("S3 Object key : " + str(s3_obj))
                            # Checks if the file exists in the S3 bucket
                            try:
                                s3_head_object_response = s3_client.head_object(Bucket=bucket_name, Key=key)
                                size_in_bytes = s3_head_object_response['ContentLength']
                                logger.info("S3 Object Size in Bytes : " + str(size_in_bytes))
                                if s3_head_object_response['ResponseMetadata'][
                                    'HTTPStatusCode'] == 200 and size_in_bytes > 0:
                                    logger.info("The given S3 object {0} exists in the s3 bucket".format(str(s3_obj)))
                                    # getting the actual file form S3
                                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                                    # Validating the MD5 checksum value
                                    logger.info("Validating the MD5 checksum value....")
                                    s3_object_md5 = md5_checksum(response)
                                    #get_s3_etag(response)
                                    # Adding Attributes to the new dict based on the filename
                                    processed_md5['app_name'] = s3_obj.split('/')[0]
                                    processed_md5['table_name'] = s3_obj.split('/')[1]
                                    processed_md5['aws_md5'] = s3_object_md5
                                    processed_md5['time_stamp'] = str(datetime.now())
                                    logger.info(
                                        "Checking whether On-Prem MD5 checksum value is provided in the indexfile or "
                                        "not....")
                                    if len(on_prem_md5) > 0:
                                        logger.info("On-prem MD5 Value : " + str(on_prem_md5))
                                        logger.info("S3 MD5 Value : " + str(s3_object_md5))
                                        if on_prem_md5 == s3_object_md5:
                                            logger.info("Both the MD5 Value are same.")
                                            processed_md5['status'] = "pass"
                                        else:
                                            logger.error("Both the MD5 Value are different.")
                                            processed_md5['status'] = "fail"
                                    else:
                                        logger.error(
                                            "The given file {0} does not contain On-prem value".format(str(file_name)))
                                        processed_md5['status'] = "fail"
                            except Exception as e:
                                logger.error(
                                    "The given file {0} does not exists in the S3 bucket.".format(str(file_name)))
                                logger.error(e)
                                # Adding Attributes to the new dict based on the filename
                                processed_md5['app_name'] = s3_obj.split('/')[0]
                                processed_md5['table_name'] = s3_obj.split('/')[1]
                                processed_md5['aws_md5'] = ''
                                processed_md5['time_stamp'] = str(datetime.now())
                                processed_md5['status'] = "fail"
                            out = s3_obj + '|' + on_prem_md5 + '|' + processed_md5['app_name'] + '|' + processed_md5[
                                'table_name'] + '|' + processed_md5['aws_md5'] + '|' + processed_md5['status'] + '|' + \
                                  processed_md5['time_stamp']
                            logger.info("Processed MD5 value : " + str(out))
                            with open(output_filename, 'a') as fd:
                                fd.write('%s\n' % out)
                                fd.close()
                        # Uploading the processed file to the S3
                        object_name = 'md5processed/' + output_filename
                        logger.info(
                            "Uploading file {0} to S3 bucket {1}.".format(str(output_filename), str(bucket_name)))
                        try:
                            s3_client.upload_file(output_filename, bucket_name, object_name)
                            logger.info("Processed Index File Uploaded Successfully!!.")
                        except Exception as e:
                            logger.error("Processed Index File Uploaded Failed!!.")
                            logger.error(e)
                    else:
                        logger.error("Index file provided in the argument is either empty or does not exist")
                else:
                    logger.error("Index file name not provided in the argument")
            except Exception as e:
                logger.error(e)
            upload_log(bucket_name)
        else:
            logger.error("Bucket name not provided in the argument")
    except Exception as e:
        logger.error(e)
