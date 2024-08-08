import findspark
from src.main.utility.encrypt_decrypt import decrypt

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *
from resources.dev.config import *

def spark_session():

    spark = SparkSession.builder.master("local[*]") \
        .appName("Retail-Project")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .config('spark.sql.adaptive.enabled' , False) \
        .getOrCreate()


    logger.info("spark session %s",spark)
    return spark