import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import ingest
import transform
import persist
import logging
import logging.config


class Pipeline:
    logging.config.fileConfig("resources/configs/logging.conf")
    def run_pipeline(self):
        logging.info('run_pipeline method started')
        ingest_process = ingest.Ingest(self.spark)
        df = ingest_process.ingest_data()
        df.show()
        tranform_process = transform.Transform(self.spark)
        transformed_df = tranform_process.transform_data(df)
        transformed_df.show()
        persist_process = persist.Persist(self.spark)
        persist_process.persist_data(transformed_df)
        logging.info('run_pipeline method ended')
        return

    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("my first spark app")\
            .enableHiveSupport().getOrCreate()

if __name__ == '__main__':
    logging.debug('Debugging the Application ')
    logging.info('Application started')
    logging.warning('Application started with warning')
    logging.error('Application started with error')

    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info('Spark Session created')
    pipeline.run_pipeline()
    logging.info('Pipeline executed')


