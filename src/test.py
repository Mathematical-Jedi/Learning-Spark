#%%
from pathlib import Path
this_file = Path(__file__).resolve()
this_directory = this_file.parent
project_directory = this_directory.parent
import sys
sys.path.append(project_directory.as_posix())
#%%
from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("test")\
        .getOrCreate()
from random import random
from operator import add
with spark:
    partitions = 100
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))
# %%
