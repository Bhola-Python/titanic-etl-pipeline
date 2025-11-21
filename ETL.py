from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("ETL").config("spark.jars", "file:///E:/SparkJars/mysql-connector-j-9.5.0.jar").getOrCreate()

# Extract
df = spark.read.csv(r'E:\Datastore\Titanic_Dataset.csv',header=True,inferSchema=True)

# Transform 

renew = df.drop("Cabin") # Droping Cabin

avg_age = renew.groupBy("Sex").agg(f.avg("Age").alias("avg_age")) # column with average value


joined = renew.join(avg_age, on="Sex", how="left") # join with the average column


final = joined.withColumn("Age",when(col("Age").isNull(), col("avg_age")).otherwise(col("Age"))).drop("avg_age") # Replacing null value with average value

newdone = final.na.fill({"Embarked": "S"}) # Filliing value with majority value

newdone.select([sum(col(c).isNull().cast("int")).alias(c)
                for c in newdone.columns]).show() # Check if any nulls left

# Write Using Jdbc
newdone.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/titanicdb") \
    .option("dbtable", "titanic_cleaned") \
    .option("user", "your_mysql_user")\
    .option("password", "your_mysql_password")\
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()

print("DATA LOADED SUCCESSFULLY INTO MYSQL!")
