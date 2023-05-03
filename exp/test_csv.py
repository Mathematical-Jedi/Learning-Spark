
#%%
from pathlib import Path
this_file = Path(__file__).resolve()
this_directory = this_file.parent
project_directory = this_directory.parent
import sys
sys.path.append(project_directory.as_posix())
#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession\
        .builder\
        .appName("test")\
        .getOrCreate()
from random import random
from operator import add
#%%
from pyspark.sql.types import *
schema = StructType([
 StructField("out_time", TimestampType(), False),
 StructField("admin_region", StringType(), False), 
 StructField("in_time", TimestampType(), False),
 StructField("berthage", IntegerType(), False),
 StructField("section", StringType(), False),
 ])
#%%
data_path = (project_directory/"data/parking_data_sz.csv").as_posix()
df = spark.read.csv(data_path,
                    header=True, 
                    # inferSchema=True)
                    schema=schema)
# 重新列排序
df = df.select('in_time', 'out_time', 'berthage', 'section', 'admin_region')
df.show(10)
df.printSchema()

#%%
# 热身练习
df.columns
df.select(expr("berthage + 1")).show(10)
df.select(col("berthage")*2).show(10)
#%% 
# 保留之前的column，增加一个新的column
df.withColumn("isBig", col("berthage")>1000).show(2)
df.withColumn(
    'section in region',
    (concat(col('section'), lit(' in '), col('admin_region')))
).select('section in region').show(2)
df.sort(col('berthage').desc()).show(2)