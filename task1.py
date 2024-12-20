from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

spark=SparkSession.builder.appName("task1").getOrCreate()
data=spark.read.csv("file:///home/siwenyu/桌面/Spark-lab4/data/user_balance_table.csv", header=True, inferSchema=True).rdd
selected_col=['user_id','report_date','total_purchase_amt','total_redeem_amt']

header=data.first()
data_task1=data.filter(lambda x:x!=header)
data_task1=data_task1.map(lambda x:(x['report_date'],(x['total_purchase_amt'],x['total_redeem_amt']))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
# to df
df=data_task1.map(lambda x:(x[0],x[1][0],x[1][1])).toDF(['report_date','total_purchase_amt','total_redeem_amt'])
df.show()
df.coalesce(1).write.csv("file:///home/siwenyu/桌面/Spark-lab4/res/task1_1",header=True)


