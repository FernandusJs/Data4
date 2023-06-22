from configparser import ConfigParser
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, HiveContext

#Configparser is a helper class to read properties from a configuration file
config = ConfigParser()
config.read('config.ini') #Define connection properties is the config file

cn = "default" #This is the default connection-name. Create a "default" profile in config.ini

#Returns a jdbc connection string based on the connection properties. Works only for sqlServer connections.
def create_jdbc():
    return f"jdbc:sqlserver://{config.get(cn, 'host')}:{config.get(cn, 'port')};database={config.get(cn, 'database')};encrypt=true;trustServerCertificate=true"

# Set the connectionName that has to be used (if you don't want to use the default profile
def set_connection(connectionName):
    global cn
    cn = connectionName

#Returns a specific property from the connection profile in the config.ini
def get_Property(propertyName):
  return config.get(cn, propertyName)

def startLocalCluster(appName, partitions=4):
    builder = SparkSession.builder \
        .appName(appName) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", partitions) \
        .master("local[*]")

    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2","com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"]
    builder = configure_spark_with_delta_pip(builder, extra_packages=extra_packages)
    spark = builder.getOrCreate()
    print(spark.getActiveSession())
    return spark