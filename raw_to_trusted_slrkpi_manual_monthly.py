from pyspark.sql.types import StructType,StructField, StringType,IntegerType
import sys
from pyspark.sql import SparkSession
from pysparkutil.common.schema import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from watcherlogger.logger import watcherlogger
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.job import Job
import traceback
import pynamodb
from pynamodb.models import Model, GlobalSecondaryIndex
from pynamodb.connection import Connection
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute, JSONAttribute
from pynamodb.indexes import KeysOnlyProjection

args = getResolvedOptions(sys.argv, ["JOB_NAME","ENV"])
ENV= args["ENV"]

# create boto3 dynamodb client connection with default AWS profile
connection = Connection()
class Status:
    todo = 0
    failed = 1
    success = 2
    
# Define the Dynamodb Status Tracker ORM data model
class StatusIndex(GlobalSecondaryIndex):
    """
    GSI for query by status
    """
    class Meta:
        index = "status-index"
        projection = KeysOnlyProjection
    status = NumberAttribute(hash_key=True)
    uri = UnicodeAttribute()
    
class JobIdIndex(GlobalSecondaryIndex):
    """
    GSI for query by job id
    """
    class Meta:
        index = "job_id-index"
        projection = KeysOnlyProjection
    job_id = UnicodeAttribute(hash_key=True)
    uri = UnicodeAttribute()
    
class ETLMetricsTrackerLambda(Model):
    class Meta:
        """
        declare metadata about the table
        """
        table_name = ""+ENV+"-raw-to-trusted-slrkpi-manual-monthly"
        region = "us-gov-west-1"
        # billing mode
        # doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html
        # use this if you want to use pay as you go mode (Recommended)
        billing_mode = pynamodb.models.PAY_PER_REQUEST_BILLING_MODE
    # define attributes
    uri = UnicodeAttribute(hash_key=True)
    dataset_name = UnicodeAttribute(default="unknown")
    status = NumberAttribute(default=Status.todo)
    target_uri = UnicodeAttribute(null=True)
    job_id = UnicodeAttribute(null=True)
    job_start_time = UTCDateTimeAttribute(null=True)
    job_end_time = UTCDateTimeAttribute(null=True)
    job_duration = NumberAttribute(null=True)
    raw_file_size = NumberAttribute(null=True)
    trusted_file_size = NumberAttribute(null=True)
    size_difference = NumberAttribute(null=True)
    source_record_count = NumberAttribute(null=True)
    target_record_count = NumberAttribute(null=True)
    record_count_delta = NumberAttribute(null=True)
    errors = JSONAttribute(default=lambda: dict())

ETLMetricsTrackerLambda.create_table(wait=True)

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
        remove_prefix = filepath.replace("s3://","")
        s3_bucket_index = remove_prefix.find("/")
        s3_bucket = remove_prefix[:s3_bucket_index]
        s3_key = ''.join(remove_prefix[s3_bucket_index+1:])
        total_size = 0
        for obj in boto3.resource('s3').Bucket(s3_bucket).objects.filter(Prefix=s3_key):
            total_size += obj.size
        context["src_file_size"] = total_size
        logger.info(context)
        
    @classmethod
    def log_target_file_size(cls, filepath, context, logger):
        import boto3
        s3 = boto3.client('s3')
        remove_prefix = filepath.replace("s3://","")
        s3_bucket_index = remove_prefix.find("/")
        s3_bucket = remove_prefix[:s3_bucket_index]
        s3_key = ''.join(remove_prefix[s3_bucket_index+1:])
        total_size = 0
        for obj in boto3.resource('s3').Bucket(s3_bucket).objects.filter(Prefix=s3_key):
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
        end_time = datetime.now()+ timedelta(hours=-5)
        time_diff = (end_time - begin)
        context["running_duration"] = time_diff.total_seconds() * 1000
        logger.info(context)

class RawDataSource(object):
    def __init__(self, params):
        self.params = params

    def read_source_data(self):
        schema_obj = Schema()
        schema_obj.set_location(self.params["input_schema_path"])
        sep = self.params["delimiter"]
        if sep == "comma" :
            file_delimeter = ","
        elif sep == "pipe":
            file_delimeter = "|"
        elif sep == "tab":
            file_delimeter = "\\t"
        elif sep == "tilde":
            file_delimeter = "~"
            
        schema = schema_obj.get_schema()
        df = spark.read.format(self.params["source_format"]).option("delimiter",file_delimeter).option("header","true").schema(schema).load(self.params["input_data_path"])
        # df = spark.read.format(self.params["source_format"]).option("delimiter",file_delimeter).option("header","true").option("inferSchema","true").option("mergeSchema","true").option("recursiveFileLookup","true").load(self.params["input_data_path"])
        return df
    
    def add_partition_raw_data(self, df):
        return df.withColumn("partition_load_dt_tmstmp",lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
class RawDataSink(object):
    def __init__(self, params):
        self.params = params
    def write_target_data(self, df):
        df.write.format("parquet").mode("overwrite").save(self.params["output_data_path"])
        return df
        
class AuditFields(object):
    def __init__(self):
        self.audit_fields = ["audit_created_tmstmp","audit_updated_tmstmp","audit_created_by","audit_updated_by","audit_data_source"]
    def add_audit_created_tmstmp(self,df,col_name):
        return df.withColumn(col_name, lit(datetime.now()))
    def add_audit_updated_tmstmp(self,df,col_name):
        return df.withColumn(col_name, lit(datetime.now()))  
    def add_audit_created_by(self,df,col_name):
        return df.withColumn(col_name, lit("glue"))
    def add_audit_updated_by(self,df,col_name):
        return df.withColumn(col_name, lit("glue"))  
    def add_audit_data_source(self, df, col_name):
        return df.withColumn(col_name, lit("slr"))  
    def add_audit(self, df):
        for col in self.audit_fields:
            col_name = "add_"+col
            fn = getattr(self, col_name)
            df = fn(df,col)
        return df    

class ArchiveData(object):
    def __init__(self, params):
        self.params = params
    def archive_data(self):
        #Get latest partition
        src_path = self.params["input_data_path"]
        s3 = boto3.resource('s3')
        s3_bucket_index = src_path.replace("s3://","").find("/")
        s3_bucket = src_path[5:s3_bucket_index+5]
        s3_key = src_path[s3_bucket_index+6:]
        bucket = s3.Bucket(s3_bucket)
        objs = list(bucket.objects.filter(Prefix=s3_key))
        for obj in objs:
            bucket_name = obj.bucket_name
            key = obj.key
            if key[-1] != '/':
                s3.meta.client.copy({'Bucket':bucket_name,'Key':key}, bucket_name, key.replace('interim','processed'))
                s3.Object(bucket_name, key).delete()
    
def underscore_space(df):
    for colname in df.schema.names:
        df = df.withColumnRenamed(colname,colname.replace(' ','_'))
    return df

def dynamo_logging():
    etl_tracker_uri = INPUT_DATA_PATH
    try:
        # this is executed when this salesforce API call that already been processed
        # but may success / failed / etc ...
        etl_tracker: ETLMetricsTrackerLambda = ETLMetricsTrackerLambda.get(etl_tracker_uri)
        # only proceed when the status == TODO
        if etl_tracker.status != Status.todo:
            return
    except ETLMetricsTrackerLambda.DoesNotExist:
        etl_tracker: ETLMetricsTrackerLambda = ETLMetricsTrackerLambda(
            uri="s3://"+etl_tracker_uri,
            source_record_count = 0,
            job_start_time=start_time,
            dataset_name = ''+ENV+'-Monthly-Manual-SLR-Measures'
        )
        etl_tracker.save()
    try:
        etl_tracker.status = Status.success
        etl_tracker.target_uri = "s3://"+OUTPUT_DATA_PATH
        etl_tracker.source_record_count = context['src_record_count']
        etl_tracker.target_record_count = context['target_record_count']
        etl_tracker.record_count_delta = context['row_diff']
        etl_tracker.raw_file_size = context['src_file_size']
        etl_tracker.trusted_file_size = context['target_file_size']
        etl_tracker.size_difference = context['byte_diff']
        etl_tracker.job_duration = context['running_duration']
        
    except Exception as e:
        etl_tracker.errors = {
            "traceback": traceback.format_exc()
        }
        etl_tracker.status = Status.failed
    etl_tracker.save()


s3 = boto3.client('s3')
s3r = boto3.resource('s3')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.pathOptionBehavior.enabled", "true")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
metrics_bucket=""+ENV+"-application-log"
INPUT_DATA_PATH = ""+ENV+"-raw-pmo/Monthly-Manual-SLR-Measures/interim/"
OUTPUT_DATA_PATH = ""+ENV+"-trusted-pmo/Monthly-Manual-SLR-Measures/interim/"
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

context = {"job_name":args["JOB_NAME"], "run_id":args["JOB_RUN_ID"], "service_arn":"SLRLoadMetric", "module_name":"SLR", "job_type":"full"}
logger = watcherlogger().Builder().setLogLevel(logging.INFO).setStreamNamePrefix(context["module_name"]).getOrCreate()
logger.start(context)

start_time = datetime.now()+ timedelta(hours=-5)
LoggerUtil.log_start_time(start_time, context, logger)

raw_params = {"input_data_path":f"s3://{INPUT_DATA_PATH}","input_schema_path":f"s3://"+ENV+"-data-dictionary/job-config/schema/Monthly_Manual_SLR_Measures.avsc","source_format":"csv","delimiter":"tilde"}
trusted_params = {"output_data_path":f"s3://{OUTPUT_DATA_PATH}","input_schema_path":f"s3://"+ENV+"-data-dictionary/job-config/schema/Monthly_Manual_SLR_Measures.avsc","catalog_db":"testdb","target_format":"parquet"}
source = RawDataSource(raw_params).read_source_data()
LoggerUtil.log_source_row_count(source, context, logger)
LoggerUtil.log_src_file_size(raw_params["input_data_path"], context, logger)

source = underscore_space(source)

trusted = RawDataSink(trusted_params).write_target_data(source)

LoggerUtil.log_target_row_count(trusted, context, logger)
LoggerUtil.log_target_file_size(trusted_params["output_data_path"], context, logger)
LoggerUtil.log_row_diff(context, logger)
LoggerUtil.log_byte_diff(context, logger)
LoggerUtil.log_running_duration(start_time, context, logger)
ArchiveData(raw_params).archive_data()
logger.success(context)

dynamo_logging()