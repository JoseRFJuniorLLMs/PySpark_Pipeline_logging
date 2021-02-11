import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config

class Ingest:
    logging.config.fileConfig("resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def ingest_data(self):
        logging.info('Ingesting from csv')
        customer_df = self.spark.read.csv("retailstore.csv",header=True)
        logging.info('DataFrame created')
        return customer_df






