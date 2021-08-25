import sys
import json
import datetime
import boto3 
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'fileName', 'fileContent'])
##args['TempDir'] = "s3://aws-glue-temporary-186975573818-us-east-1/chaitanya.rudraraju/"
job = Job(glueContext)
job.init("test", args)

"""
s3 = boto3.client("s3")
response = s3.list_objects(Bucket = 'talentinc.resumedata.test', Marker = 'test_jsons/')
checksum = response['Contents'][1]["Key"].split('/')[-1].split('.')[0]

obj = s3.get_object(Bucket= 'talentinc.resumedata.test', Key=response['Contents'][1]["Key"])
file_content = obj['Body'].read().decode('utf-8')
json_content = json.loads(file_content)
"""
def flatten_json(y):
    out = {}
  
    def flatten(x, name =''):

        if type(x) is dict:
              
            for a in x:
                flatten(x[a], name + a + '.')

        elif type(x) is list:
              
            i = 0
              
            for a in x:                
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
  
    flatten(y)
    return out

json_content = json.loads(args['fileContent'])
a=flatten_json(json_content)

dc={}

dc['key'] = [i for i in a]
dc['value'] = [str(a[i]) for i in a]
dc['checksum'] = [args['fileName']]*len(dc['key'])
dc['created_at'] = [datetime.datetime.now()]*len(dc['key'])
dc['updated_at'] = [datetime.datetime.now()]*len(dc['key'])

df = pd.DataFrame.from_dict(dc)
ddf = spark.createDataFrame(df)
ddf1 = DynamicFrame.fromDF(ddf, glueContext, "ddf1")

applymapping1 = ApplyMapping.apply(frame = ddf1, mappings = [("checksum", "string", "checksum", "string"), ("key", "string", "key", "string"), ("value", "string", "value", "string"), ("created_at", "timestamp", "created_at", "timestamp"), ("updated_at", "timestamp", "updated_at", "timestamp")], transformation_ctx = "applymapping1")
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["checksum", "key", "value", "created_at", "updated_at"], transformation_ctx = "selectfields2")
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "resumedataredshift", table_name = "warehouse_public_pancake", transformation_ctx = "resolvechoice3")
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "resumedataredshift", table_name = "warehouse_public_pancake", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()
