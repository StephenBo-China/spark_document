from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, HiveContext

def load_sql_file(sql_file):
    with open(sql_file, 'r') as isf:
        sql_txt = isf.readlines()
        return "".join(sql_txt)

def init_spark_session():
    spark = SparkSession \
            .builder \
            .appName("offline_estimate_1") \
            .config("spark.yarn.queue", "root.celuemoxingbu_gulfstream") \
            .enableHiveSupport() \
            .getOrCreate()
    return spark

def save_hive(data, table_name):
    from pyspark.sql import SparkSession, HiveContext
    data.registerTempTable('test_hive')
    sqlContext.sql("create table " + table_name + " select * from test_hive")
    return
