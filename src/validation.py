from pyspark.sql.functions import col, isnan, when, count
from src.logger import get_logger

logger = get_logger(__name__)


class DataValidation:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def validate_account_ids(self, transactions_df, accounts_df):
        # Ensure all account_id in transactions exist in accounts
        joined = transactions_df.join(accounts_df.select("account_id"), on="account_id", how="left_anti")
        count_invalid = joined.count()
        if count_invalid > 0:
            logger.error(f"Found {count_invalid} transactions with invalid account_id")
            raise ValueError("Data validation failed: invalid account IDs in transactions")
        logger.info("Account ID validation passed")

    def validate_balances(self, accounts_df):
        # For Savings and Checking, balance must be >= 0
        invalid_balances = accounts_df.filter(
            (col("account_type").isin("Savings", "Checking")) & (col("balance").cast("double") < 0)
        )
        count_invalid = invalid_balances.count()
        if count_invalid > 0:
            logger.error(f"Found {count_invalid} accounts with negative balance")
            raise ValueError("Data validation failed: negative balance for Savings/Checking accounts")
        logger.info("Account balance validation passed")

    def validate_transactions_amount(self, transactions_df):
        # Remove transactions with null or zero amount
        filtered_df = transactions_df.filter((col("amount").isNotNull()) & (col("amount").cast("double") != 0))
        dropped = transactions_df.count() - filtered_df.count()
        if dropped > 0:
            logger.warning(f"Dropped {dropped} transactions with null or zero amount")
        return filtered_df
