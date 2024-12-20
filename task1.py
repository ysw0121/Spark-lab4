from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

spark=SparkSession.builder.appName("task1").getOrCreate()
data=spark.read.csv("file:///home/siwenyu/桌面/Spark-lab4/data/user_balance_table.csv", header=True, inferSchema=True).rdd
selected_col=['user_id','report_date','total_purchase_amt','total_redeem_amt']

header=data.first()
data_task=data.filter(lambda x:x!=header)

def func(task_id):
    if task_id==1:
        # task1_1
        data_task1=data_task.map(lambda x:(x['report_date'],(x['total_purchase_amt'],x['total_redeem_amt']))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
        # to df
        df=data_task1.map(lambda x:(x[0],x[1][0],x[1][1])).toDF(['report_date','total_purchase_amt','total_redeem_amt'])
        df.show()
        df.coalesce(1).write.mode("overwrite").csv("file:///home/siwenyu/桌面/Spark-lab4/res/task1_1",header=True)
    elif task_id==2:
        # task1_2
        # active user_id according to recorder date num >=5 in august
        data_task2=data_task.filter(lambda x:x['report_date']>=20140801 and x['report_date']<=20140831).map(lambda x:(x['user_id'],1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=5).map(lambda x:x[0])

        # num of user id in active user_id
        num=data_task2.count()
        print(f"num of active user in august is \n {num}")
    
    else:
        pass

func(1)
func(2)

