from pyspark.sql.functions import col
from src.logger import get_logger

logger = get_logger(__name__)


class FraudDetection:
    def __init__(self, config):
        self.config = config

    def detect_high_value_transactions(self, transactions_df):
        df = transactions_df.filter(col("amount").cast("double").abs() > self.config.FRAUD_AMOUNT_THRESHOLD)
        logger.info(f"Detected {df.count()} high-value transactions exceeding threshold")
        return df

    def detect_unusual_merchants(self, transactions_df):
        df = transactions_df.filter(col("merchant").isin(self.config.FRAUD_MERCHANTS))
        logger.info(f"Detected {df.count()} transactions with unusual merchants")
        return df

    def detect_rapid_frequency(self, transactions_df):
        # Example: Transactions in rapid succession by same account (within 1 hour)
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lag, unix_timestamp

        win = Window.partitionBy("account_id").orderBy("transaction_date")
        df = transactions_df.withColumn("prev_txn_time", lag("transaction_date").over(win))
        df = df.withColumn("txn_gap_sec",
                           (unix_timestamp(col("transaction_date")) - unix_timestamp(col("prev_txn_time"))))
        flagged = df.filter(col("txn_gap_sec") < 3600)
        logger.info(f"Detected {flagged.count()} transactions with rapid frequency (under 1 hour gap)")
        return flagged
