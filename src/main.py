import os
from pyspark.sql import SparkSession
from src.config import load_config
from src.logger import get_logger
from src.data_ingestion import DataIngestion
from src.validation import DataValidation
from src.transformations import Transformations
from src.fraud_detection import FraudDetection
from src.storage import Storage

logger = get_logger(__name__)


def main():
    spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    config = load_config()

    ingestion = DataIngestion(spark, config)
    validation = DataValidation(spark, config)
    transform = Transformations(spark)
    fraud = FraudDetection(config)
    storage = Storage(config)

    try:
        # Load raw data (bronze ingestion)
        customers_df = ingestion.read_customers()
        accounts_df = ingestion.read_accounts()
        transactions_df = ingestion.read_transactions()

        # Write raw data into bronze if needed
        # ingestion.write_bronze(customers_df, "customers")
        # ingestion.write_bronze(accounts_df, "accounts")
        # ingestion.write_bronze(transactions_df, "transactions")

        # Data validation
        validation.validate_account_ids(transactions_df, accounts_df)
        validation.validate_balances(accounts_df)
        transactions_df = validation.validate_transactions_amount(transactions_df)

        # Data cleaning / transformation for silver
        silver_tran_df = transactions_df.dropDuplicates(["transaction_id"])  # sample transformation
        storage.write_silver(silver_tran_df, "transactions_silver")

        # Customer 360 view and KPIs
        customer_360_df = transform.create_customer_360(customers_df, accounts_df, silver_tran_df)
        storage.write_silver(customer_360_df, "customer_360")

        credit_debit_agg = transform.aggregate_credits_debits_by_customer(customer_360_df)
        storage.write_gold(credit_debit_agg, "customer_credit_debit")

        daily_txn_volume = transform.calculate_daily_transaction_volume_per_merchant(silver_tran_df)
        storage.write_gold(daily_txn_volume, "daily_txn_volume")

        avg_balance = transform.calculate_avg_balance_per_account_type(accounts_df)
        storage.write_gold(avg_balance, "avg_balance")

        cltv = transform.calculate_customer_lifetime_value(credit_debit_agg)
        storage.write_gold(cltv, "customer_lifetime_value")

        # Fraud detection - save flagged transactions for review
        high_value_txns = fraud.detect_high_value_transactions(silver_tran_df)
        unusual_merchant_txns = fraud.detect_unusual_merchants(silver_tran_df)
        rapid_freq_txns = fraud.detect_rapid_frequency(silver_tran_df)

        # Save flagged for monitoring or alerting
        # storage.write_gold(high_value_txns, "fraud_high_value")
        # storage.write_gold(unusual_merchant_txns, "fraud_unusual_merchant")
        # storage.write_gold(rapid_freq_txns, "fraud_rapid_frequency")

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
