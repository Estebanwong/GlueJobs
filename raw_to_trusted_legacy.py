from pyspark.sql.types import StringType
import sys
from watcherlogger.logger import watcherlogger
import boto3
from pyspark.sql import SparkSession
from pysparkutil.common.schema import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Relationalize
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import time
from datetime import datetime, timedelta
from pyspark.conf import SparkConf

sys_args = getResolvedOptions(sys.argv, [])


class LoggerUtil(object):
    def __init__(self):
        self.name="LoggerUtil"

    @classmethod    
    def log_start_time(cls, start_time, context, logger):
        context["start_time"] = str(start_time)
        logger.info(context)

    @classmethod    
    def log_duration(cls, start_time, context, logger):
        context["duration"] = str(datetime.now() + timedelta(hours=-5) - start_time)
        logger.info(context)
    
    @classmethod    
    def log_source_row_count(cls, df, context, logger):
        context["src_record_count"] = df.count()
        logger.info(context)

    @classmethod    
    def log_target_row_count(cls, df, context, logger):
        context["target_record_count"] = df.count()
        logger.info(context)

    @classmethod
    def log_src_file_size(cls, filepath, context, logger):
        import boto3
        s3 = boto3.client('s3')
        s3_bucket_index = filepath.replace("s3://","").find("/")
        s3_bucket = filepath[5:s3_bucket_index+5]
        s3_key = '/'.join(filepath[s3_bucket_index+6:].split('/')[:-1])
        total_size = 0
        contents = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)['Contents']
        for file in contents:
            if file['Key'][-3:] in ['txt']:
                total_size += file['Size']
        context["src_file_size"] = total_size       
        logger.info(context)
        
    @classmethod
    def log_target_file_size(cls, filepath, context, logger):
        import boto3
        s3 = boto3.client('s3')
        s3_bucket_index = filepath.replace("s3://","").find("/")
        s3_bucket = filepath[5:s3_bucket_index+5]
        s3_key = '/'.join(filepath[s3_bucket_index+6:].split('/')[:-1])
        total_size = 0
        for obj in boto3.resource('s3').Bucket(s3_bucket).objects.filter(Prefix=f"{s3_key}/"):
            total_size += obj.size
        context["target_file_size"] = total_size
        logger.info(context)
        
    @classmethod
    def log_row_diff(cls,context, logger):
        context["row_diff"] = context["src_record_count"]-context["target_record_count"]
        logger.info(context)
    
    @classmethod
    def log_byte_diff(cls,context, logger):
        context["byte_diff"] = context["src_file_size"]-context["target_file_size"]
        logger.info(context)
        
    
    @classmethod
    def log_file_count(cls, counter, context, logger):
        context["file_count"] = counter
        logger.info(context)
    
    @classmethod
    def log_running_duration(cls, begin, context, logger):
        context["running_duration"] = str(datetime.now()+ timedelta(hours=-5) - begin)
        logger.info(context)
        

def create_metrics_file(start_time):
    s3_res = boto3.resource('s3')
    current_log_data = s3_res.Object(metrics_bucket, logFileName)
    current_log_data_body = current_log_data.get()
    current_log_data_content = current_log_data_body['Body'].read().decode('UTF-8')
    end_time = datetime.now()+ timedelta(hours=-5)
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds() * 1000
    current_log_data_content += table + ',' + str(context['start_time']) + ',' + str(context['src_record_count']) + ',' + str(context['target_record_count'])+ ',' + str(context['row_diff']) + ',' + str(context['src_file_size']) + ',' + str(context['target_file_size'])+ ',' + str(context['byte_diff'])+ ',' + str(context['duration'])+ '\n'
    s3_res.Object(metrics_bucket, logFileName).put(Body=current_log_data_content)

        
def execute_logging(start_time,source_df,target_df,in_path,out_path):
    LoggerUtil.log_start_time(start_time, context, logger)
    LoggerUtil.log_duration(start_time, context, logger)
    LoggerUtil.log_source_row_count(source_df, context, logger)
    LoggerUtil.log_target_row_count(target_df, context, logger)
    LoggerUtil.log_row_diff(context, logger)
    LoggerUtil.log_src_file_size(f"s3://{in_path}", context, logger)
    LoggerUtil.log_target_file_size(f"s3://{out_path}", context, logger)
    LoggerUtil.log_byte_diff(context, logger)
        

def truncate_cr(spark, in_path, out_path,db,table):
    
    #There is a pipe and \r\n at the end of each row so this immediately organizes the data into its proper rows
    original_count = spark.read.text(f"s3://{in_path}")

    start_time = datetime.now() + timedelta(hours=-5)
    counts = original_count.count()
    db_underscores = db.replace('-','_')
    table_underscores = table.replace('.','')

    #Filtering out data with rows <2 and just moving it into a parquet without transforming
    if counts==1:
        df = spark.read.csv(f"s3://{in_path}",inferSchema =True, header=True, sep='|')

        df.write.format("parquet").mode("overwrite").option("inferSchema", "true").option("path",f"s3://{out_path}").save(f"{db_underscores}.{table_underscores}")
        # df.write.format("parquet").mode("overwrite").option("inferSchema", "true").option("path",f"s3://{out_path}").saveAsTable(f"{db_underscores}_trusted.{table_underscores}")
        #df.write.format("parquet").mode("overwrite").option("inferSchema", "true").option("path",f"s3://{out_path}").save()

        #Log in Cloudwatch
        execute_logging(start_time,original_count,df,in_path,out_path)

        #Create Metrics file
        create_metrics_file(start_time)
    elif counts == 0:
        pass
    else:
        df = spark.read.option("lineSep", '|\r\n').text(f"s3://{in_path}")
        first_two_rows=df.take(2)
        header = first_two_rows[0][0].split('|')
        #If there's only one column pass (this is for 1099 for now)
        if len(header) == 1:
            pass
        else:
            logger.start(context)
            find_schema = first_two_rows[1][0].split('|')
            no_header = df.filter(col(df.schema.names[0]) != first_two_rows[0][0])
            #Carriage return removal
            replace_rn= no_header.withColumn("value", regexp_replace(no_header["value"],'\r\n',''))
            replace_r = replace_rn.withColumn("value", regexp_replace(replace_rn["value"],'\r',''))
            no_more_returns = replace_r.withColumn("value", regexp_replace(replace_r["value"],'\n',' '))
            #We were asked to remove the quotes from all data
            no_quotes = no_more_returns.withColumn("value", regexp_replace(no_more_returns["value"],'"',''))

            #Convert the single column text dataframe to a table that removes the delimiter
            split_col = split(no_quotes['value'], '\\|')

            for i,val in enumerate(header):
                if find_schema[i][0]=='"':
                    no_quotes=no_quotes.withColumn(val,split_col.getItem(i).cast("string"))
                else:
                    no_quotes=no_quotes.withColumn(val,split_col.getItem(i).cast("int"))

            #Drops the single column text dataframe and writes out as an parquet and athena table
            multi_col = no_quotes.drop('value')
            multi_col.write.format("parquet").mode("overwrite").option("inferSchema", "true").option("path",f"s3://{out_path}").saveAsTable(f"{db_underscores}.{table_underscores}")
            #multi_col.write.format("parquet").mode("overwrite").option("inferSchema", "true").option("path",f"s3://{out_path}").save()
            #Log in Cloudwatch            
            execute_logging(start_time,original_count,multi_col,in_path,out_path)

            #Creates Metric file
            create_metrics_file(start_time)

args = getResolvedOptions(sys.argv, ["INPUT_DATA_PATH","OUTPUT_DATA_PATH","prefixes", "metrics_bucket","ENV"])
ENV= args["ENV"]

# args = {"INPUT_DATA_PATH": "converge-dev-raw", "OUTPUT_DATA_PATH":"dev-trusted-legacy"}
# prefixes = ['alight-legacy-dataload-2/','alight-legacy-dataload-2-wave2/']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.pathOptionBehavior.enabled", "true")

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


input_data = args["INPUT_DATA_PATH"]
output_data = args["OUTPUT_DATA_PATH"]
in_bucket = input_data

metrics_bucket = args["metrics_bucket"]
prefixes = args["prefixes"].split(",")

counter = 0
begin = datetime.now() + timedelta(hours=-5)
s3 = boto3.client('s3')
for prefix in prefixes:
    contents = s3.list_objects_v2(Bucket=in_bucket, Prefix=prefix)['Contents']
    for file in contents:
        logFileName = "curation/" + in_bucket + "/" + prefix + "metrics.csv"
        filename = file['Key']
        table = filename.split('/')[1]
        db = filename.split('/')[0]
        table_list=[]
        if filename[-3:] in ['csv','txt','CSV','TXT'] and table[-7:] != 'updated' and filename not in table_list:
            table_list.append(table)
            
            ####
            df = spark.read.option('lineSep','|\r\n').text(f"s3://{input_data}/{filename}")
            first_two_rows=df.take(2)
            header = first_two_rows[0][0].split('|')
            header_count = len(header)
            
            df2 = spark.read.option('lineSep','"|\r\n').text(f"s3://{input_data}/{filename}")
            df2= df2.withColumn("value", concat(col("value"),lit('"')))
            first_row = df.take(1)
            no_header = df2.filter(col(df2.schema.names[0]) != first_row[0][0])
            
            first_row_data = spark.createDataFrame(first_two_rows[1][0],StringType())
            df3 = first_row_data.union(no_header)
            split_col = split(df3['value'], '\\|')
            first_column=df3.withColumn("first",split_col.getItem(0).cast("string"))
            pipe_count=first_column.withColumn('pipeCount', size(split(col('value'), "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))).drop('value')
            bad_row_too_long_count = pipe_count.filter(pipe_count.pipeCount>header_count)
            good_row_count = pipe_count.filter(pipe_count.pipeCount==header_count).count()-1
            bad_row_too_short_count = pipe_count.filter(pipe_count.pipeCount<header_count)
            long_count = bad_row_too_long_count.count()
            short_count = bad_row_too_short_count.count()
            
            print(f"{table} has {header_count} columns typically")
            # print(bad_row_too_short_count.show(900,False))
            # print(bad_row_too_long_count.show(30407,False))
            print("short,long,good")
            print(f"{short_count},{long_count},{good_row_count}")
            ####
            
            # #Start Logging
            # context = {"job_name":f"{table}", "run_id":sys_args["JOB_RUN_ID"], "service_arn":"NA", "module_name":"carriage_return", "job_type":"full"}
            # logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
            # _path = f"{input_data}/{filename}"

            # #Cleaning Function starts
            # truncate_cr(spark, _path, f"{output_data}/{filename}",db,table)

            # #Logging file count and 
            # counter +=1
            # LoggerUtil.log_file_count(counter, context, logger)
            # LoggerUtil.log_running_duration(begin, context, logger)
            # logger.success(context)