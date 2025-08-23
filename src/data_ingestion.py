from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.logger import get_logger

logger = get_logger(__name__)


class DataIngestion:
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config

    def read_customers(self):
        try:
            df = self.spark.read.option("header", True).csv(self.config.CUSTOMER_FILE)
            logger.info("Loaded customer data")
            return df
        except Exception as e:
            logger.error(f"Error loading customers: {e}")
            raise

    def read_accounts(self):
        try:
            df = self.spark.read.option("header", True).csv(self.config.ACCOUNT_FILE)
            logger.info("Loaded account data")
            return df
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            raise

    def read_transactions(self, date_filter=None):
        try:
            df = self.spark.read.option("header", True).csv(self.config.TRANSACTION_FILE)

            if date_filter:
                df = df.filter(col("transaction_date") >= date_filter)
                logger.info(f"Filtered transactions on or after {date_filter}")

            logger.info("Loaded transaction data")
            return df
        except Exception as e:
            logger.error(f"Error loading transactions: {e}")
            raise

    def write_bronze(self, df, table_name):
        try:
            df.write.format("parquet").mode("append").save(self.config.BRONZE_PATH + table_name)
            logger.info(f"Wrote data to bronze layer: {table_name}")
        except Exception as e:
            logger.error(f"Error writing bronze data for {table_name}: {e}")
            raise
