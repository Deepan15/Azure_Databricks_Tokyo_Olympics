# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "a2ff70e0-8c24-4001-b26a-5b94ce2bfc54",
"fs.azure.account.oauth2.client.secret": 'w..8Q~d59Hht31773a8NUkmjZjVbhoMlKjRYBcsG',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/541f16ad-2ba9-44de-aef9-d265eb285a0e/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdatadeepan.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)


athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw_data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw_data/coaches.csv")
gender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw_data/gender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw_data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw_data/teams.csv")

# Getting the athletes, coaches, gender, medals, teams data the specific location 
#.option(header, true): It is used to treat the first row as header option.
#.format: It is used to give the format of the source file.
#.load: It is used to load the data from(mountpoint/name of the container/name of the file)
#.option(inferSchema, true): It will go through the csv automatically and try to understand the schema of every column


gender.show()
# Show command is used to view the csv data in a tabular format
+--------------------+------+----+-----+
|          Discipline|Female|Male|Total|
+--------------------+------+----+-----+
|      3x3 Basketball|    32|  32|   64|
|             Archery|    64|  64|  128|
| Artistic Gymnastics|    98|  98|  196|
|   Artistic Swimming|   105|   0|  105|
|           Athletics|   969|1072| 2041|
|           Badminton|    86|  87|  173|
|   Baseball/Softball|    90| 144|  234|
|          Basketball|   144| 144|  288|
|    Beach Volleyball|    48|  48|   96|
|              Boxing|   102| 187|  289|
|        Canoe Slalom|    41|  41|   82|
|        Canoe Sprint|   123| 126|  249|
|Cycling BMX Frees...|    10|   9|   19|
|  Cycling BMX Racing|    24|  24|   48|
|Cycling Mountain ...|    38|  38|   76|
|        Cycling Road|    70| 131|  201|
|       Cycling Track|    90|  99|  189|
|              Diving|    72|  71|  143|
|          Equestrian|    73| 125|  198|
|             Fencing|   107| 108|  215|
+--------------------+------+----+-----+
only showing top 20 rows


athletes.printSchema()
#printSchema: It is used to display the Table name with datatype
root
 |-- PersonName: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Discipline: string (nullable = true)


gender = gender.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male", col("Male").cast(IntegerType()))\
    .withColumn("Total", col("Total").cast(IntegerType()))
#Here we are converting the datatype from string to integer for female, male and total columns
#withColumn(): It is a transformation function which is used to change the value, convert datatype of an existing column,
#create a new column etc.
#cast: It is used to convert the column into any datatype



# Find the top countries with highest number of gold medals: 
top_gold_medal = medals.orderBy("Gold" , ascending = False).show()
top_gold_medal = medals.orderBy("Gold" , ascending = False).select("TeamCountry", "Gold").show()
#Here we are using orderby to get the gold column in descending to get the highest gold medal count.
# If we want only two columns then use the select option which is used in the 2nd code. 


# Calculate the average number of entries by gender for each discipline. 
average_entries_by_gender = gender.withColumn(
    'Avg_Female', gender['Female'] / gender['Total']
).withColumn(
    'Avg_Male', gender['Male'] / gender['Total']
)
average_entries_by_gender.show()
#Here we are using with column to select and create a new column named Avg_female and Avg_male.
#To calculate average dividing the female count by total count and same for male.
#And finally using show() to display the output.


athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/coaches")
gender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/gender")
medals.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/medals")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed_data/teams")

 # repartition: It is a method where it will save the entire file as separate parts in the destnation(transform_data).
    #if we give it as 2 then if the total data is 100 1st 50 will be in a separate file and the next 50 will be in a separate file.
        
# mode(overwrite): It is used so that we can edit/rewrite and rerun the code as many times so that it won't throw any errors.
    #if we didn't use it we cannot able to run more than 1 time.

# .option("header",'true'): It is used to treat the first row as header.

#.csv("/mnt/tokyoolympic/transformed_data/athletes"): .csv is to load the data as csv format.
#("mountpoint/name of the container/name of the file to be saved in target")

gender.withColumnRenamed("Total", "Total_Count")
# withColumnRename: It is used to change the column name 
#syntax: table_name.withColumnRenamed(existingcolumnname, newcolumnname)
