from pyspark.sql.functions import col, sum as _sum, avg, count, expr
from src.logger import get_logger

logger = get_logger(__name__)

class Transformations:
    def __init__(self, spark):
        self.spark = spark

    def create_customer_360(self, customers_df, accounts_df, transactions_df):
        # Join all datasets for unified view
        df = customers_df.join(accounts_df, "customer_id", "left") \
                         .join(transactions_df, "account_id", "left")
        logger.info("Created customer 360 view")
        return df

    def aggregate_credits_debits_by_customer(self, customer_360_df):
        df = customer_360_df.groupBy("customer_id").agg(
            _sum(expr("case when transaction_type = 'Credit' then amount else 0 end")).alias("total_credits"),
            _sum(expr("case when transaction_type = 'Debit' then amount else 0 end")).alias("total_debits")
        )
        logger.info("Aggregated total credits and debits by customer")
        return df

    def calculate_daily_transaction_volume_per_merchant(self, transactions_df):
        df = transactions_df.groupBy("transaction_date", "merchant").agg(
            count("transaction_id").alias("daily_transaction_volume")
        )
        logger.info("Calculated daily transaction volume per merchant")
        return df

    def calculate_avg_balance_per_account_type(self, accounts_df):
        df = accounts_df.groupBy("account_type").agg(
            avg(col("balance").cast("double")).alias("avg_balance")
        )
        logger.info("Calculated average balance per account type")
        return df

    def calculate_customer_lifetime_value(self, aggregated_credits_debits_df):
        df = aggregated_credits_debits_df.withColumn(
            "customer_lifetime_value",
            col("total_credits") + col("total_debits")  # assuming debits are negative amounts
        )
        logger.info("Calculated customer lifetime value")
        return df
