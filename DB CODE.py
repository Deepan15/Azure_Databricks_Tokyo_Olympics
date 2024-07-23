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


