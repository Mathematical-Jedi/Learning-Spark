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
# 数据清洗
# 先看看数据有哪些问题
df.count() # 1048001 清洗前
# 首先关于time的问题
print(df.where(col('out_time') < col('in_time')).count()) # 349
print(df.where(col('out_time') <= col('in_time')).count()) # 77076
# 结束时间正好等于开始时间的挺多的。
# 我们还看下有没有异常的 time 比如开始时间特别早或者特别晚
# df.sort(col('in_time').asc()).show(5)  #18年9月
# df.sort(col('in_time').desc()).show(5) #19年3月
df.sort(col('out_time').asc()).show(5)  #18年9月
df.sort(col('out_time').desc()).show(5) #19年3月


#%%
print(df.where(isnull('out_time') 
               | isnull('in_time')
               | isnull('section')
               | isnull('admin_region')
               | isnull('berthage')
               ).count())
print(df.where(isnull('berthage')).count())
# 刚才发现 1条空的bertahge，我们看看是什么回事
df.where(isnull('berthage')).show()
# 其他都正常，就是不知道哪个停车场
#%%
# 正式进行筛选
df = df.where(col('berthage').isNotNull())\
    .where(col('out_time') > col('in_time'))

#%%
# 问题0，增加一个新的column，记录停车时长
# https://stackoverflow.com/questions/72576979/how-can-i-extract-nanosecond-from-interval-day-to-second-in-pyspark
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.DayTimeIntervalType.html
# 我们想要把 INTERVAL DAY TO SECOND 类型 转换成秒
# 怎么办呢？

# 智能机器人：你可以用SECOND函数，但是SECOND函数只能用在TIMESTAMP类型上，所以你需要先把INTERVAL DAY TO SECOND类型转换成TIMESTAMP类型，然后再用SECOND函数。
# 用户：那么怎么转换类型呢？
# 智能机器人：你可以用CAST函数，但是CAST函数只能用在TIMESTAMP类型上，所以你需要先把INTERVAL DAY TO SECOND类型转换成TIMESTAMP类型，然后再用CAST函数。
# 用户：那second是什么函数？怎么调用呢？
#  

# 失败的操作
# df.withColumn('parking_time', expr("(out_time - in_time) as DOUBLE"))\
# df.withColumn('parking_time', col('out_time')-col('in_time'))\
# df.withColumn('parking_time', (col('out_time')-col('in_time')).SECOND)\
# df.withColumn('parking_time', second(cast(TimestampType, col('out_time')-col('in_time')))  )\
# df.withColumn('parking_time',  timestamp_seconds((col('out_time')-col('in_time'))))\
# 第一种办法，把timestamp转换为unix时间戳，unix时间戳是整数，单位为秒。原本TimeStamp类型是字符串存储的。
# df.withColumn('parking_time', (unix_timestamp(col('out_time')) - unix_timestamp(col('in_time'))).cast('double'))\
df = df.withColumn('parking_time', 
    (unix_timestamp(col('out_time')) 
     - unix_timestamp(col('in_time')))\
    .cast('long'))
df.show(5)
#%%
# 问题1
# df.groupby('section').count().show()
df.groupby('section').count()\
    .toPandas()\
    .to_csv("r1.csv", header=True)
# pandas得到单个文件。如果是write函数，可能得到多个文件。
#%% 
# 问题2
# 找到所有unique的berthage，同时找到每个berthage对应的那一个section
# 一个bertahge有很多条记录，每个记录都有section，我们选择哪个呢?
# 答，我们可以选择众数，即出现次数最多的那个section，用的是agg函数
df.select('berthage', 'section')\
    .groupby('berthage')\
    .agg(mode('section').alias('section'))\
    .toPandas().to_csv('r2.csv', header=True)
    # .show()
    
#%%
# 问题3
df.select('section', 'parking_time')\
    .groupby('section')\
    .agg(mean('parking_time').cast('long').alias('avg_parking_time'))\
    .toPandas().to_csv('r3.csv', header=True)
    # .show()
#%%
# 问题4
df.select('berthage', 'parking_time')\
    .groupby('berthage')\
    .agg(mean('parking_time').alias('avg_parking_time'))\
    .sort(col('avg_parking_time').desc())\
    .select('berthage', col('avg_parking_time').cast('long'))\
    .toPandas().to_csv('r4.csv', header=True)

#%%
# 问题5
# 对于每一个section
#  从00:00:00-24:00:00, 所有的半小时区间内
#   找到section中有车的停车场占总的数量的比。
# 推导
# - 对于一个记录，开始或者结束时间在区间内 <=> 该记录在区间内，该berthage有车
import numpy as np
s = df.select('section', 'berthage', 'in_time', 'out_time', 'parking_time')
group = s.groupby('section') # 两个东西共同决定一个组。
total_lots = group.agg(countDistinct(col('berthage')).alias('count'))
total_lots.show()
#%%

# 首先先把停车时间和00:00的差距的小时写出来
# 第一种写法 是 hour， minute等函数从 TimeStamp类型提取东西
# s = s.withColumn('out_hour', hour(col('out_time'))+
#                  minute(col('out_time'))/60+second(col('out_time')/3600))\ 
# 第二种写法是
for k in ['in', "out"]:
    s = s.withColumn(f'{k}_date', to_timestamp(to_date(f'{k}_time')))\
    .withColumn(f'{k}_hour', (col(f'{k}_time').cast('Long')-col(f'{k}_date').cast('Long'))/3600)
s = s.withColumn('greater_than_12h', col('parking_time')>=12*3600)
s = s.withColumn(
    'out_hour', when(
        col('out_hour')<col('in_hour'), 
        col('out_hour')+24
    ).otherwise(col('out_hour'))
)
# s.toPandas().to_csv('see_s.csv')
# s.where(col('out_hour')>24).repartition(1).write.csv('see_s.csv')
s.where('greater_than_12h').repartition(1).write.csv('see_s.csv')
#%%
res = []
for start_hour in np.arange(0, 24, 0.5):
    end_hour = start_hour+0.5
    print(f'正在处理[{start_hour}, {end_hour}) 区间内的数据。')
    # 筛选在指定时间段内的数据。
    q = s.where( (col('out_hour')>end_hour) 
           |
                )
    group = q.groupby('section')
    has_car_lots =group.agg(countDistinct(col('berthage')).alias('has_car_count'))
    res.append(has_car_lots)
    
# %%
spark.stop()