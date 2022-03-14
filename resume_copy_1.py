import boto3
from time import sleep
from db_connection import *


secrets = get_secret()
secrets = secrets.replace("\n", "")
secrets = secrets.replace(" ", "")
secrets = json.loads(secrets)


con = get_db_conn(secrets)
cur = con.cursor()



sns = boto3.client("sns",region_name="us-east-1")

 
try:
    def resume_copy(t,n,count_array,split_table,table_list,schema):
        global parquet_buck
        for i in range(t,n):
            if (count_array[i] == 'T'):
                
                sleep(1)
                table = split_table[i]
                parquet_buck = table_list[i]
                print("copy started ", parquet_buck)
                logger.info("Copy started for " + str(parquet_buck))
                
                print(table_list[i])
                copy_command = ("COPY " +schema+'.'+ table + " FROM '" + 's3://statefarm-data-migration-snowball-testing/' + parquet_buck + "' IAM_ROLE '" + "arn:aws:iam::958785264664:role/sf_poc_redshift_role'" + "FORMAT AS PARQUET;")
                #print(copy_command)
                sns.publish(TopicArn='arn:aws:sns:us-east-1:958785264664:SNSProcessedEvent',Message="Successfully Initiated copying for " + str(parquet_buck) + " to redshift.",Subject='SFHTC Copy to Redshift alert-Copying parquet file to redshift')                
                cur.execute(copy_command)
                f = open("resume.txt", "w")
                f.write(str(i))
                sns.publish(TopicArn='arn:aws:sns:us-east-1:958785264664:SNSProcessedEvent',Message="Successfully copied "+str(parquet_buck)+" to redshift.",Subject='SFHTC Copy to Redshift alert-Copying parquet file Successful')
                print('copying completed')
                logger.info("Copy completed for " + str(parquet_buck))
                con.commit()

        else:
            f = open("resume.txt", "w")
            f.write('E')
            f.close()
except Exception as e:
    print(e)
    sns.publish(TopicArn='arn:aws:sns:us-east-1:958785264664:SNSProcessedEvent',Message="Copy of " + str(parquet_buck) + "Interrupted with error : "+e,Subject='SFHTC Copy to Redshift alert - Copying parquet file Interrupted')
    logger.info("copying failed for the parquet file : "+str(parquet_buck)+"with an error : ")
    logger.info(e)