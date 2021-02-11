import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
class Persist:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def persist_data(self,df):
        logger = logging.getLogger("Persist")
        logger.info('Persisting')
        logger.error("dummy error in Persisting")
        df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")
