from pyspark.sql.functions import col, hour, minute, floor, explode, count, countDistinct, sum
from pyspark.sql.types import FloatType

# 读取数据
df = spark.read.format('csv').option('header', True).load('data.csv')

# 计算停车时间
df = df.withColumn('parking_time', (col('out_time').cast('long')
                                    - col('in_time').cast('long')) / 60)

# 以半小时为单位划分时间区间
df = df.withColumn('half_hour', floor((hour(col('in_time')) * 60
                                       + minute(col('in_time'))) / 30))

# 按照section、half_hour、berthage进行分组统计
agg_df = df.groupBy('section', 'half_hour', 'berthage').agg(
    countDistinct('berthage').alias('N_s'),
    sum((col('parking_time') >= col('half_hour') * 30) &
        (col('parking_time') < (col('half_hour') + 1) * 30)).alias('N_si')
)

# 计算百分比
agg_df = agg_df.withColumn('percentage', agg_df['N_si'] / agg_df['N_s'] * 100)

# 提取时间区间的开始和结束时间
time_df = df.select(floor((hour(col('in_time')) * 60 +
                    minute(col('in_time'))) / 30).alias('half_hour')).distinct()
time_df = time_df.withColumn('start_time', time_df['half_hour'] * 30)
time_df = time_df.withColumn('end_time', (time_df['half_hour'] + 1) * 30)

# 将时间区间与section进行笛卡尔积，与agg_df进行左连接，得到完整的结果
result_df = time_df.crossJoin(df.select('section').distinct()).join(
    agg_df, ['section', 'half_hour'], 'left')
result_df = result_df.select('start_time', 'end_time', 'section',
                             'N_s', 'N_si', 'percentage').orderBy('section', 'start_time')

result_df.show()

input("按回车继续...")
