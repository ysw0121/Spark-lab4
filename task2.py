from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit

spark=SparkSession.builder.appName("task2").getOrCreate()

data_profile=spark.read.csv("file:///home/siwenyu/桌面/Spark-lab4/data/user_profile_table.csv", header=True, inferSchema=True)
# creat view
data_profile.createOrReplaceTempView("user_profile_table")

data_balance=spark.read.csv("file:///home/siwenyu/桌面/Spark-lab4/data/user_balance_table.csv", header=True, inferSchema=True)
data_balance.createOrReplaceTempView("user_balance_table")



query1="""
SELECT up.City, AVG(ub.tBalance) AS average_balance
FROM user_profile_table up
JOIN user_balance_table ub on up.user_id=ub.user_id
WHERE ub.report_date = '20140301'
GROUP BY up.City
ORDER BY average_balance DESC;
"""
res1=spark.sql(query1)
res1.show()





query2="""

SELECT CityID, user_id, total_flow
FROM (
    SELECT 
        up.City AS CityID, 
        ub.user_id, 
        SUM(ub.total_purchase_amt + ub.total_redeem_amt) AS total_flow,
        RANK() OVER (PARTITION BY up.City ORDER BY SUM(ub.total_purchase_amt + ub.total_redeem_amt) DESC) AS rank
    FROM user_profile_table up
    JOIN user_balance_table ub ON up.user_id = ub.user_id
    WHERE ub.report_date LIKE '201408%'
    GROUP BY up.City, ub.user_id
) t
WHERE rank <= 3;
"""
res2=spark.sql(query2)
res2.show()

res1.coalesce(1).write.mode("overwrite").csv("file:///home/siwenyu/桌面/Spark-lab4/res/task2_1",header=True)
res2.coalesce(1).write.mode("overwrite").csv("file:///home/siwenyu/桌面/Spark-lab4/res/task2_2",header=True)