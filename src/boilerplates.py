from pathlib import Path
import os
this_directory = Path(os.getcwd()).resolve()
project_directory = this_directory.parent
output_directory = project_directory/"output"
if not output_directory.exists():
    output_directory.mkdir()
import sys
sys.path.append(project_directory.as_posix())
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession\
        .builder\
        .appName("test")\
        .getOrCreate()
from random import random
from operator import add
from pyspark.sql.types import *
schema = StructType([
 StructField("index", IntegerType(), False),
 StructField("in_time", TimestampType(), False),
 StructField("out_time", TimestampType(), False),
 StructField("berthage", IntegerType(), False),
 StructField("section", StringType(), False),
 StructField("admin_region", StringType(), False), 
 StructField("parking_time", LongType(), False),
 ])
data_path = (project_directory/"data/clean_parking_data.csv").as_posix()
df = spark.read.csv(data_path,
                    header=True, 
                    inferSchema=True,
                    
                    )
                #     schema=schema)
df.limit(5).show(5)


import csv
import pyspark.pandas as pp
# 把spark dataframe转换成单个 csv 文件的方法
# write + repartition : 会形成一个文件夹，里面有个很丑的文件
# toPandas + to_csv : 速度慢，pandas格式和spark不兼容，比如datetime64[ns]会变成object，直接报错
# 直接手动写一个csv导出器
def write_df(df, name:str):
    file_path = (output_directory/name).as_posix()
    # single = df.repartition(1)
    single = df
    """ Converts spark dataframe to CSV file """
    with open(file_path, "w") as f:
        fieldnames=single.columns
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(dict(zip(fieldnames, fieldnames)))
        for row in single.toLocalIterator(): # Returns an iterator that contains all of the rows in this DataFrame
            writer.writerow(row.asDict())
    print(f"written to {file_path}")
    
    
# df.columns
# for column in df.columns:
#     pass
    # print(f'col_{column} = col("{column}")')
    # exec(f'col_{column} = col("{column}")')
col_index = col("index")
col_in_time = col("in_time")
col_out_time = col("out_time")
col_berthage = col("berthage")
col_section = col("section")
col_admin_region = col("admin_region")
col_parking_time = col("parking_time")