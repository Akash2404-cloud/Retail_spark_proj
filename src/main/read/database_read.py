from resources.dev.config import *
from src.main.utility.spark_session import *

class DatabaseReader:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def create_dataframe(self,spark,table_name):
        df = spark.read.jdbc(url=self.url,
                             table=table_name,
                             properties=self.properties)
        return df



# spark = spark_session()
# db_reader = DatabaseReader(url , properties)
# customer_df = db_reader.create_dataframe(spark,customer_table_name)
# customer_df.show()