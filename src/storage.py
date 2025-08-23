from src.logger import get_logger

logger = get_logger(__name__)


class Storage:
    def __init__(self, config):
        self.config = config

    def write_bronze(self, df, table_name):
        try:
            df.write.format("parquet").mode("append").save(self.config.BRONZE_PATH + table_name)
            logger.info(f"Wrote to bronze layer: {table_name}")
        except Exception as e:
            logger.error(f"Error writing to bronze: {e}")
            raise

    def write_silver(self, df, table_name):
        try:
            df.write.format("parquet").mode("overwrite").save(self.config.SILVER_PATH + table_name)
            logger.info(f"Wrote to silver layer: {table_name}")
        except Exception as e:
            logger.error(f"Error writing to silver: {e}")
            raise

    def write_gold(self, df, table_name):
        try:
            df.write.format("parquet").mode("overwrite").save(self.config.GOLD_PATH + table_name)
            logger.info(f"Wrote to gold layer: {table_name}")
        except Exception as e:
            logger.error(f"Error writing to gold: {e}")
            raise
