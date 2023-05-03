# Learn Spark 笔记

## 第二章

strings.show(10, truncate=False)
https://spark.apache.org/docs/3.4.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html?highlight=show
truncate 表示 大于 20 字符的字符串被裁剪。
truncate可以自己设置长度
vertical 可以垂直显示

得到的结果是一列叫做“value”的数组。

read.text 得到一个Dataframe，而不是RDD
